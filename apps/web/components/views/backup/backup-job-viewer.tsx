"use client";

import { useState, useEffect, useRef } from "react";
import { Dialog, DialogContent, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { ScrollArea } from "@/components/ui/scroll-area";
import { CheckCircle2, XCircle, Loader2, Clock, Copy, Download, AlertCircle } from "lucide-react";
import { streamBackupLogs, getBackupLogs, getBackup } from "@/lib/api/backups";
import { DownloadBackupDialog } from "./download-backup-dialog";
import { useQuery } from "@tanstack/react-query";
import { useToast } from "@/hooks/use-toast";
import { cn } from "@/lib/utils";

interface BackupJobViewerProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  backupId: string | null;
  backup?: {
    id: string;
    status: string;
    path?: string;
    s3_object_key?: string;
    size?: number;
  };
}

type StepStatus = "pending" | "in_progress" | "success" | "warning" | "error";

interface BackupStep {
  id: string;
  name: string;
  status: StepStatus;
  duration?: string;
  startTime?: number;
  endTime?: number;
}

export function BackupJobViewer({ open, onOpenChange, backupId, backup: initialBackup }: BackupJobViewerProps) {
  const { toast } = useToast();
  const [logs, setLogs] = useState<string[]>([]);
  const [isConnected, setIsConnected] = useState(false);
  const [isLoading, setIsLoading] = useState(true);
  const [showStdOut, setShowStdOut] = useState(true);
  const [showUploadProgress, setShowUploadProgress] = useState(true);
  const [downloadDialogOpen, setDownloadDialogOpen] = useState(false);
  const scrollAreaRef = useRef<HTMLDivElement>(null);
  const streamEndedRef = useRef(false);
  const stepsRef = useRef<Map<string, BackupStep>>(new Map());
  const [steps, setSteps] = useState<BackupStep[]>([]);

  // Fetch backup details if not provided
  const { data: backupData } = useQuery({
    queryKey: ['backup', backupId],
    queryFn: () => getBackup(backupId!),
    enabled: !!backupId && open && !initialBackup,
    refetchInterval: 5000, // Poll every 5 seconds for updates
  });

  const backup = initialBackup || backupData;
  
  // Debug: log backup status
  useEffect(() => {
    if (backup) {
      console.log("Backup status in BackupJobViewer:", { 
        id: backup.id, 
        status: backup.status, 
        canDownload: backup.status === "completed" || backup.status === "success" 
      });
    }
  }, [backup]);

  // Initialize steps
  useEffect(() => {
    if (!open || !backupId) return;

    const initialSteps: BackupStep[] = [
      { id: "setup", name: "Setting up Backup", status: "pending" },
      { id: "prepare", name: "Preparing Database Backup", status: "pending" },
      { id: "backup", name: "Creating Database Backup", status: "pending" },
      { id: "upload", name: "Uploading to S3 Storage", status: "pending" },
    ];

    stepsRef.current = new Map(initialSteps.map(step => [step.id, step]));
    setSteps(initialSteps);
  }, [open, backupId]);

  // Parse logs to update step status
  useEffect(() => {
    if (!logs.length) return;

    const newSteps = new Map(stepsRef.current);
    let hasChanges = false;

    logs.forEach((log) => {
      const logLower = log.toLowerCase();

      // Detect step transitions
      if (logLower.includes("starting backup") || logLower.includes("backup file:")) {
        const step = newSteps.get("setup");
        if (step && step.status === "pending") {
          step.status = "success";
          step.endTime = Date.now();
          hasChanges = true;
        }
        const prepareStep = newSteps.get("prepare");
        if (prepareStep && prepareStep.status === "pending") {
          prepareStep.status = "in_progress";
          prepareStep.startTime = Date.now();
          hasChanges = true;
        }
      }

      if (logLower.includes("backup completed successfully") || logLower.includes("backup file created")) {
        const step = newSteps.get("backup");
        if (step) {
          step.status = "success";
          step.endTime = Date.now();
          if (step.startTime) {
            const duration = ((step.endTime - step.startTime) / 1000).toFixed(0);
            step.duration = formatDuration(parseInt(duration));
          }
          hasChanges = true;
        }
        const uploadStep = newSteps.get("upload");
        if (uploadStep && uploadStep.status === "pending") {
          uploadStep.status = "in_progress";
          uploadStep.startTime = Date.now();
          hasChanges = true;
        }
      }

      if (logLower.includes("starting s3 upload") || logLower.includes("uploading to s3")) {
        const step = newSteps.get("upload");
        if (step && step.status === "pending") {
          step.status = "in_progress";
          step.startTime = Date.now();
          hasChanges = true;
        }
      }

      if (logLower.includes("successfully uploaded") || logLower.includes("upload completed successfully")) {
        const step = newSteps.get("upload");
        if (step) {
          step.status = "success";
          step.endTime = Date.now();
          if (step.startTime) {
            const duration = ((step.endTime - step.startTime) / 1000).toFixed(0);
            step.duration = formatDuration(parseInt(duration));
          }
          hasChanges = true;
        }
      }

      if (logLower.includes("failed to upload") || logLower.includes("s3 upload failed")) {
        const step = newSteps.get("upload");
        if (step) {
          step.status = "error";
          step.endTime = Date.now();
          if (step.startTime) {
            const duration = ((step.endTime - step.startTime) / 1000).toFixed(0);
            step.duration = formatDuration(parseInt(duration));
          }
          hasChanges = true;
        }
      }

      if (logLower.includes("warning") && logLower.includes("s3")) {
        const step = newSteps.get("upload");
        if (step && step.status === "in_progress") {
          step.status = "warning";
          hasChanges = true;
        }
      }

      if (logLower.includes("backup failed") || logLower.includes("[error]")) {
        const step = newSteps.get("backup");
        if (step && step.status === "in_progress") {
          step.status = "error";
          step.endTime = Date.now();
          if (step.startTime) {
            const duration = ((step.endTime - step.startTime) / 1000).toFixed(0);
            step.duration = formatDuration(parseInt(duration));
          }
          hasChanges = true;
        }
      }

    });

    // Update durations for in-progress steps
    newSteps.forEach((step) => {
      if (step.status === "in_progress" && step.startTime) {
        const duration = ((Date.now() - step.startTime) / 1000).toFixed(0);
        step.duration = formatDuration(parseInt(duration));
        hasChanges = true;
      }
    });

    if (hasChanges) {
      stepsRef.current = newSteps;
      setSteps(Array.from(newSteps.values()));
    }
  }, [logs]);

  // Auto-scroll to bottom
  useEffect(() => {
    if (scrollAreaRef.current) {
      const scrollContainer = scrollAreaRef.current.querySelector('[data-radix-scroll-area-viewport]');
      if (scrollContainer) {
        scrollContainer.scrollTop = scrollContainer.scrollHeight;
      }
    }
  }, [logs]);

  useEffect(() => {
    if (!open || !backupId) return;

    setLogs([]);
    setIsConnected(false);
    setIsLoading(true);
    streamEndedRef.current = false;

    let streamCleanup: (() => void) | null = null;
    let streamTimeout: NodeJS.Timeout;

    const fetchStoredLogs = async () => {
      try {
        setIsLoading(true);
        const storedLogs = await getBackupLogs(backupId);
        if (storedLogs && storedLogs.trim() !== '') {
          const logLines = storedLogs.split('\n').filter((line) => line.trim() !== '');
          if (logLines.length > 0) {
            setLogs(logLines);
            setIsConnected(false);
            setIsLoading(false);
            return true; // Successfully loaded logs
          }
        }
        setIsLoading(false);
        return false; // No logs found
      } catch (error) {
        console.error("Failed to fetch stored logs:", error);
        setIsLoading(false);
        return false;
      }
    };

    // For completed backups, try fetching stored logs immediately
    // Check if backup is completed by checking status
    const checkAndLoadLogs = async () => {
      // If backup exists and is completed, fetch stored logs immediately
      if (backup && backup.status && backup.status !== 'in_progress') {
        const hasLogs = await fetchStoredLogs();
        if (hasLogs) {
          // Logs loaded, don't try streaming
          return;
        }
      }

      // For active backups or if no logs found, try streaming
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
            if (!streamEndedRef.current) {
              // If streaming fails, try fetching stored logs
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

        // If no logs arrive after 2 seconds, try fetching stored logs (backup might be completed)
        streamTimeout = setTimeout(async () => {
          if (!streamEndedRef.current) {
            const currentLogs = await getBackupLogs(backupId);
            if (!currentLogs || currentLogs.trim() === '') {
              await fetchStoredLogs();
            }
          }
        }, 2000);
      };

      tryStreaming();
    };

    checkAndLoadLogs();

    return () => {
      if (streamCleanup) {
        streamCleanup();
      }
      if (streamTimeout) {
        clearTimeout(streamTimeout);
      }
    };
  }, [backupId, open, backup]);

  const formatDuration = (seconds: number): string => {
    const hours = Math.floor(seconds / 3600);
    const minutes = Math.floor((seconds % 3600) / 60);
    const secs = seconds % 60;
    return `${hours.toString().padStart(2, '0')}:${minutes.toString().padStart(2, '0')}:${secs.toString().padStart(2, '0')}`;
  };

  const getStepIcon = (status: StepStatus) => {
    switch (status) {
      case "success":
        return <CheckCircle2 className="h-5 w-5 text-green-500" />;
      case "error":
        return <XCircle className="h-5 w-5 text-red-500" />;
      case "warning":
        return <AlertCircle className="h-5 w-5 text-yellow-500" />;
      case "in_progress":
        return <Loader2 className="h-5 w-5 text-blue-500 animate-spin" />;
      default:
        return <Clock className="h-5 w-5 text-gray-400" />;
    }
  };

  const filteredLogs = logs.filter((log) => {
    if (!showStdOut && !log.includes("[STDERR]") && !log.includes("[ERROR]")) {
      return false;
    }
    if (!showUploadProgress && (log.includes("S3") || log.includes("upload"))) {
      return false;
    }
    return true;
  });

  const handleCopyLogs = () => {
    const logText = filteredLogs.join('\n');
    navigator.clipboard.writeText(logText);
    toast({
      title: "Copied",
      description: "Logs copied to clipboard",
    });
  };

  const handleDownload = (e?: React.MouseEvent) => {
    if (e) {
      e.preventDefault();
      e.stopPropagation();
    }
    if (!backupId) {
      toast({
        title: "Error",
        description: "Backup ID is missing",
        variant: "destructive",
      });
      return;
    }
    console.log("Opening download dialog for backup:", backupId);
    setDownloadDialogOpen(true);
  };

  if (!backupId) return null;

  return (
    <>
      <Dialog open={open} onOpenChange={onOpenChange}>
        <DialogContent className="max-w-7xl h-[90vh] flex flex-col p-0">
        <DialogHeader className="px-6 pt-6 pb-4 border-b">
          <DialogTitle className="text-xl">Backup Job: {backupId.slice(0, 8)}...</DialogTitle>
        </DialogHeader>

        <div className="flex-1 flex overflow-hidden">
          {/* Left Panel - Steps */}
          <div className="w-80 border-r bg-muted/30 p-4 overflow-y-auto">
            <div className="space-y-2">
              {steps.map((step) => (
                <div
                  key={step.id}
                  className={cn(
                    "p-3 rounded-lg border transition-colors",
                    step.status === "in_progress" && "bg-blue-50 dark:bg-blue-950 border-blue-200 dark:border-blue-800",
                    step.status === "success" && "bg-green-50 dark:bg-green-950 border-green-200 dark:border-green-800",
                    step.status === "error" && "bg-red-50 dark:bg-red-950 border-red-200 dark:border-red-800",
                    step.status === "warning" && "bg-yellow-50 dark:bg-yellow-950 border-yellow-200 dark:border-yellow-800",
                    step.status === "pending" && "bg-gray-50 dark:bg-gray-900 border-gray-200 dark:border-gray-800"
                  )}
                >
                  <div className="flex items-center gap-3">
                    {getStepIcon(step.status)}
                    <div className="flex-1 min-w-0">
                      <div className="font-medium text-sm">{step.name}</div>
                      {step.duration && (
                        <div className="text-xs text-muted-foreground mt-1">
                          Duration: {step.duration}
                        </div>
                      )}
                    </div>
                  </div>
                </div>
              ))}
            </div>

            {/* Options */}
            <div className="mt-6 space-y-2">
              <label className="flex items-center gap-2 text-sm cursor-pointer">
                <input
                  type="checkbox"
                  checked={showUploadProgress}
                  onChange={(e) => setShowUploadProgress(e.target.checked)}
                  className="rounded"
                />
                <span>Show Upload Progress</span>
              </label>
              <label className="flex items-center gap-2 text-sm cursor-pointer">
                <input
                  type="checkbox"
                  checked={showStdOut}
                  onChange={(e) => setShowStdOut(e.target.checked)}
                  className="rounded"
                />
                <span>Show StdOut</span>
              </label>
            </div>

            {/* File Info */}
            {backup && (
              <div className="mt-6 p-3 rounded-lg border bg-background">
                <div className="text-xs text-muted-foreground space-y-1">
                  {backup.path && (
                    <div>
                      <span className="font-medium">File Path:</span> {backup.path.split('/').pop()}
                    </div>
                  )}
                  {backup.s3_object_key && (
                    <div>
                      <span className="font-medium">Storage:</span> S3
                    </div>
                  )}
                  {backup.size !== undefined && (
                    <div>
                      <span className="font-medium">Size:</span> {formatBytes(backup.size)}
                    </div>
                  )}
                  <div>
                    <span className="font-medium">Status:</span>{" "}
                    <Badge variant={backup.status === "completed" ? "default" : "secondary"}>
                      {backup.status}
                    </Badge>
                  </div>
                </div>
              </div>
            )}
          </div>

          {/* Right Panel - Logs */}
          <div className="flex-1 flex flex-col">
            <div className="flex items-center justify-between p-4 border-b">
              <div className="flex items-center gap-2">
                {isConnected && (
                  <div className="h-2 w-2 bg-green-500 rounded-full animate-pulse" />
                )}
                <span className="text-sm text-muted-foreground">
                  {isConnected ? "Streaming logs..." : "Logs"}
                </span>
              </div>
              <div className="flex items-center gap-2">
                <Button 
                  type="button"
                  variant="outline" 
                  size="sm" 
                  onClick={handleCopyLogs}
                >
                  <Copy className="h-4 w-4 mr-2" />
                  Copy
                </Button>
                {backup && (backup.status === "completed" || backup.status === "success") && (
                  <Button 
                    type="button"
                    variant="outline" 
                    size="sm" 
                    onClick={(e) => {
                      console.log("Download button clicked in backup job viewer", { backup, backupId });
                      handleDownload(e);
                    }}
                  >
                    <Download className="h-4 w-4 mr-2" />
                    Download
                  </Button>
                )}
              </div>
            </div>

            <ScrollArea ref={scrollAreaRef} className="flex-1 p-4">
              {isLoading && logs.length === 0 ? (
                <div className="text-center text-muted-foreground py-8">Loading logs...</div>
              ) : filteredLogs.length === 0 ? (
                <div className="text-center text-muted-foreground py-8">No logs available</div>
              ) : (
                <div className="font-mono text-xs space-y-1">
                  {filteredLogs.map((log, index) => {
                    const isError = log.includes("[ERROR]") || log.includes("[STDERR]");
                    const isWarning = log.includes("[WARNING]");
                    const isInfo = log.includes("[INFO]");
                    const isSuccess = log.includes("[SUCCESS]");

                    return (
                      <div
                        key={index}
                        className={cn(
                          "whitespace-pre-wrap break-words",
                          isError && "text-red-600 dark:text-red-400",
                          isWarning && "text-yellow-600 dark:text-yellow-400",
                          isInfo && "text-blue-600 dark:text-blue-400",
                          isSuccess && "text-green-600 dark:text-green-400"
                        )}
                      >
                        {log}
                      </div>
                    );
                  })}
                </div>
              )}
            </ScrollArea>
          </div>
        </div>
        </DialogContent>
      </Dialog>

      {backupId && (
        <DownloadBackupDialog
          open={downloadDialogOpen}
          onOpenChange={setDownloadDialogOpen}
          backupId={backupId}
          backupPath={backup?.path || backup?.s3_object_key || `backup-${backupId}.sql`}
        />
      )}
    </>
  );
}

function formatBytes(bytes: number): string {
  if (bytes === 0) return "0 B";
  const k = 1024;
  const sizes = ["B", "KB", "MB", "GB"];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return Math.round(bytes / Math.pow(k, i) * 100) / 100 + " " + sizes[i];
}

