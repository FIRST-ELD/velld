"use client";

import { useState } from "react";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { AlertCircle, Database } from "lucide-react";
import { Alert, AlertDescription } from "@/components/ui/alert";
import { useConnections } from "@/hooks/use-connections";
import { useBackup } from "@/hooks/use-backup";
import type { Backup } from "@/types/backup";

interface RestoreDialogProps {
  backup: Backup | null;
  open: boolean;
  onOpenChange: (open: boolean) => void;
}

export function RestoreDialog({ backup, open, onOpenChange }: RestoreDialogProps) {
  const [selectedConnectionId, setSelectedConnectionId] = useState<string>("");
  const [targetDatabaseName, setTargetDatabaseName] = useState<string>("");
  const [useCustomDatabase, setUseCustomDatabase] = useState(false);
  const [skipChecksumVerification, setSkipChecksumVerification] = useState(false);
  const [confirmed, setConfirmed] = useState(false);
  
  const { connections, isLoading: isLoadingConnections } = useConnections();
  const { restoreBackupToDatabase, isRestoring } = useBackup();

  const handleRestore = () => {
    if (!backup || !selectedConnectionId) return;

    restoreBackupToDatabase(
      {
        backupId: backup.id,
        connectionId: selectedConnectionId,
        targetDatabaseName: useCustomDatabase && targetDatabaseName.trim() ? targetDatabaseName.trim() : undefined,
        skipChecksumVerification: skipChecksumVerification,
      },
      {
        onSuccess: () => {
          onOpenChange(false);
          setSelectedConnectionId("");
          setTargetDatabaseName("");
          setUseCustomDatabase(false);
          setConfirmed(false);
        },
      }
    );
  };

  const handleCancel = () => {
    onOpenChange(false);
    setSelectedConnectionId("");
    setTargetDatabaseName("");
    setUseCustomDatabase(false);
    setSkipChecksumVerification(false);
    setConfirmed(false);
  };

  // Reset custom database name when connection changes
  const handleConnectionChange = (connectionId: string) => {
    setSelectedConnectionId(connectionId);
    if (!useCustomDatabase) {
      const conn = connections?.find(c => c.id === connectionId);
      if (conn) {
        setTargetDatabaseName(conn.database_name || "");
      }
    }
  };

  const selectedConnection = connections?.find(
    (conn) => conn.id === selectedConnectionId
  );

  const compatibleConnections = connections?.filter(
    (conn) => conn.type === backup?.database_type
  );

  console.log(connections)
  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-[500px]">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2">
            <Database className="h-5 w-5" />
            Restore Database Backup
          </DialogTitle>
          <DialogDescription>
            Select a target connection to restore this backup.
          </DialogDescription>
        </DialogHeader>

        <div className="space-y-4 py-4">
          <div className="rounded-lg border p-3 space-y-1">
            <p className="text-sm font-medium">Backup Details</p>
            <p className="text-sm text-muted-foreground">
              Database: {backup?.database_name}
            </p>
            <p className="text-sm text-muted-foreground">
              Type: {backup?.database_type}
            </p>
            <p className="text-sm text-muted-foreground">
              Created: {backup?.created_at ? new Date(backup.created_at).toLocaleString() : 'N/A'}
            </p>
          </div>

          <div className="space-y-2">
            <Label className="text-sm font-medium">Target Connection</Label>
            <Select
              value={selectedConnectionId}
              onValueChange={handleConnectionChange}
              disabled={isLoadingConnections || isRestoring}
            >
              <SelectTrigger>
                <SelectValue placeholder="Select a database connection..." />
              </SelectTrigger>
              <SelectContent>
                {compatibleConnections && compatibleConnections.length > 0 ? (
                  compatibleConnections.map((conn) => (
                    <SelectItem key={conn.id} value={conn.id}>
                      <div className="flex items-center gap-2 min-w-0">
                        <span className="truncate max-w-[200px]" title={conn.name}>
                          {conn.name}
                        </span>
                        <span className="text-xs text-muted-foreground flex-shrink-0">
                          ({conn.type})
                        </span>
                      </div>
                    </SelectItem>
                  ))
                ) : (
                  <SelectItem value="none" disabled>
                    No compatible connections found
                  </SelectItem>
                )}
              </SelectContent>
            </Select>
          </div>

          {selectedConnectionId && (
            <>
              <div className="flex items-start space-x-2">
                <input
                  type="checkbox"
                  id="use-custom-db"
                  checked={useCustomDatabase}
                  onChange={(e) => {
                    setUseCustomDatabase(e.target.checked);
                    if (!e.target.checked) {
                      const conn = connections?.find(c => c.id === selectedConnectionId);
                      setTargetDatabaseName(conn?.database_name || "");
                    }
                  }}
                  className="h-4 w-4 rounded border-gray-300 mt-1"
                  disabled={isRestoring}
                />
                <label
                  htmlFor="use-custom-db"
                  className="text-sm leading-tight cursor-pointer flex-1"
                >
                  Restore to a different database name
                </label>
              </div>

              {useCustomDatabase && (
                <div className="space-y-2">
                  <Label htmlFor="target-db-name" className="text-sm font-medium">
                    Target Database Name
                  </Label>
                  <Input
                    id="target-db-name"
                    value={targetDatabaseName}
                    onChange={(e) => setTargetDatabaseName(e.target.value)}
                    placeholder={selectedConnection?.database_name || "Enter database name"}
                    disabled={isRestoring}
                    className="font-mono"
                  />
                  <p className="text-xs text-muted-foreground">
                    The database must already exist. If it doesn&apos;t exist, create it first on the target server.
                  </p>
                </div>
              )}

              {!useCustomDatabase && selectedConnection && (
                <div className="rounded-lg border p-3 bg-muted/50">
                  <p className="text-sm font-medium">Target Database</p>
                  <p className="text-sm text-muted-foreground font-mono">
                    {selectedConnection.database_name}
                  </p>
                </div>
              )}
            </>
          )}

          {selectedConnectionId && (
            <>
              <Alert>
                <AlertCircle className="h-4 w-4" />
                <AlertDescription className="text-sm">
                  <strong>Important:</strong> The target database must exist. If you&apos;re restoring to a different database name, make sure to create it first on the target server.
                </AlertDescription>
              </Alert>
              
              <div className="flex items-start space-x-2">
                <input
                  type="checkbox"
                  id="skip-checksum"
                  checked={skipChecksumVerification}
                  onChange={(e) => setSkipChecksumVerification(e.target.checked)}
                  className="h-4 w-4 rounded border-gray-300 mt-1"
                  disabled={isRestoring}
                />
                <label
                  htmlFor="skip-checksum"
                  className="text-sm leading-tight cursor-pointer flex-1"
                >
                  Skip checksum verification (use if you&apos;re getting checksum errors but know the file is correct)
                </label>
              </div>
            </>
          )}

          <div className="flex items-start space-x-2">
            <input
              type="checkbox"
              id="confirm-restore"
              checked={confirmed}
              onChange={(e) => setConfirmed(e.target.checked)}
              className="h-4 w-4 rounded border-gray-300"
              disabled={!selectedConnectionId || isRestoring}
            />
            <label
              htmlFor="confirm-restore"
              className="text-sm leading-tight cursor-pointer"
            >
              Confirm restore to <strong>{selectedConnection?.name || 'selected connection'}</strong>
              {useCustomDatabase && targetDatabaseName && (
                <> (database: <strong className="font-mono">{targetDatabaseName}</strong>)</>
              )}
            </label>
          </div>
        </div>

        <DialogFooter>
          <Button
            variant="outline"
            onClick={handleCancel}
            disabled={isRestoring}
          >
            Cancel
          </Button>
          <Button
            onClick={handleRestore}
            disabled={!selectedConnectionId || !confirmed || isRestoring}
          >
            {isRestoring ? "Restoring..." : "Restore Backup"}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
