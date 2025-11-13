"use client";

import { useEffect, useRef, useState } from "react";
import { Card } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { ScrollArea } from "@/components/ui/scroll-area";
import { X, Copy, Check } from "lucide-react";
import { streamBackupLogs, getBackupLogs } from "@/lib/api/backups";
import { cn } from "@/lib/utils";

interface BackupLogViewerProps {
  backupId: string;
  open: boolean;
  onClose: () => void;
}

export function BackupLogViewer({ backupId, open, onClose }: BackupLogViewerProps) {
  const [logs, setLogs] = useState<string[]>([]);
  const [isConnected, setIsConnected] = useState(false);
  const [isLoading, setIsLoading] = useState(true);
  const [copiedIndex, setCopiedIndex] = useState<number | null>(null);
  const scrollAreaRef = useRef<HTMLDivElement>(null);
  const logsEndRef = useRef<HTMLDivElement>(null);
  const streamEndedRef = useRef(false);

  useEffect(() => {
    if (!open || !backupId) return;

    setLogs([]);
    setIsConnected(false);
    setIsLoading(true);
    streamEndedRef.current = false;

    // Try streaming first (for active backups)
    let streamCleanup: (() => void) | null = null;
    let streamTimeout: NodeJS.Timeout;

    const tryStreaming = () => {
      streamCleanup = streamBackupLogs(
        backupId,
        (log) => {
          setLogs((prev) => [...prev, log]);
          setIsConnected(true);
          setIsLoading(false);
        },
        async (error) => {
          console.error("Log stream error:", error);
          setIsLoading(false);
          // If stream fails, try to fetch stored logs
          if (!streamEndedRef.current) {
            await fetchStoredLogs();
          }
        },
        async () => {
          setIsConnected(false);
          streamEndedRef.current = true;
          // After stream ends, fetch stored logs to ensure we have everything
          await fetchStoredLogs();
        }
      );

      // If no logs arrive after 3 seconds, try fetching stored logs (backup might be completed)
      streamTimeout = setTimeout(async () => {
        if (!streamEndedRef.current) {
          await fetchStoredLogs();
        }
      }, 3000);
    };

    const fetchStoredLogs = async () => {
      try {
        setIsLoading(true);
        const storedLogs = await getBackupLogs(backupId);
        if (storedLogs) {
          const logLines = storedLogs.split('\n').filter(line => line.trim() !== '');
          if (logLines.length > 0) {
            setLogs(logLines);
            setIsConnected(false); // Not streaming, so not "connected"
          }
        }
      } catch (error) {
        console.error("Failed to fetch stored logs:", error);
        setLogs((prevLogs) => {
          if (prevLogs.length === 0) {
            return ["[ERROR] Failed to load backup logs"];
          }
          return prevLogs;
        });
      } finally {
        setIsLoading(false);
      }
    };

    tryStreaming();

    return () => {
      if (streamCleanup) {
        streamCleanup();
      }
      if (streamTimeout) {
        clearTimeout(streamTimeout);
      }
    };
  }, [backupId, open]);

  useEffect(() => {
    // Auto-scroll to bottom when new logs arrive
    if (logsEndRef.current) {
      logsEndRef.current.scrollIntoView({ behavior: "smooth" });
    }
  }, [logs.length]);

  const copyToClipboard = async (text: string, index: number) => {
    try {
      await navigator.clipboard.writeText(text);
      setCopiedIndex(index);
      setTimeout(() => setCopiedIndex(null), 2000);
    } catch (err) {
      console.error("Failed to copy:", err);
    }
  };

  const copyAllLogs = async () => {
    try {
      await navigator.clipboard.writeText(logs.join("\n"));
      setCopiedIndex(-1);
      setTimeout(() => setCopiedIndex(null), 2000);
    } catch (err) {
      console.error("Failed to copy:", err);
    }
  };

  if (!open) return null;

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50">
      <Card className="w-full max-w-4xl h-[80vh] flex flex-col m-4">
        <div className="flex items-center justify-between p-4 border-b">
          <div className="flex items-center gap-2">
            <h2 className="text-lg font-semibold">Backup Logs</h2>
            <div className={cn(
              "h-2 w-2 rounded-full",
              isConnected ? "bg-green-500" : "bg-gray-400"
            )} />
            <span className="text-sm text-muted-foreground">
              {isConnected ? "Connected" : "Disconnected"}
            </span>
          </div>
          <div className="flex items-center gap-2">
            {logs.length > 0 && (
              <Button
                variant="outline"
                size="sm"
                onClick={copyAllLogs}
                className="gap-2"
              >
                {copiedIndex === -1 ? (
                  <>
                    <Check className="h-4 w-4" />
                    Copied
                  </>
                ) : (
                  <>
                    <Copy className="h-4 w-4" />
                    Copy All
                  </>
                )}
              </Button>
            )}
            <Button variant="ghost" size="sm" onClick={onClose}>
              <X className="h-4 w-4" />
            </Button>
          </div>
        </div>
        <ScrollArea className="flex-1 p-4" ref={scrollAreaRef}>
          <div className="space-y-1 font-mono text-sm">
            {isLoading && logs.length === 0 ? (
              <div className="text-muted-foreground">Loading logs...</div>
            ) : logs.length === 0 ? (
              <div className="text-muted-foreground">No logs available for this backup</div>
            ) : (
              logs.map((log, index) => {
                const isError = log.includes("[ERROR]");
                const isWarning = log.includes("[WARNING]");
                const isStderr = log.includes("[STDERR]");
                const isStreamEnded = log.includes("[STREAM ENDED]");

                return (
                  <div
                    key={index}
                    className={cn(
                      "group flex items-start gap-2 p-2 rounded hover:bg-muted/50",
                      isError && "text-red-500",
                      isWarning && "text-yellow-500",
                      isStderr && "text-orange-500",
                      isStreamEnded && "text-muted-foreground font-semibold"
                    )}
                  >
                    <span className="text-muted-foreground text-xs min-w-[60px]">
                      {String(index + 1).padStart(4, "0")}
                    </span>
                    <span className="flex-1 break-words">{log}</span>
                    <Button
                      variant="ghost"
                      size="sm"
                      className="opacity-0 group-hover:opacity-100 h-6 w-6 p-0"
                      onClick={() => copyToClipboard(log, index)}
                    >
                      {copiedIndex === index ? (
                        <Check className="h-3 w-3" />
                      ) : (
                        <Copy className="h-3 w-3" />
                      )}
                    </Button>
                  </div>
                );
              })
            )}
            <div ref={logsEndRef} />
          </div>
        </ScrollArea>
      </Card>
    </div>
  );
}

