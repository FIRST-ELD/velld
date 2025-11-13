"use client";

import { useState, useEffect } from "react";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Database, Info } from "lucide-react";
import { useSettings } from "@/hooks/use-settings";
import { Skeleton } from "@/components/ui/skeleton";
import { UpdateSettingsRequest } from "@/types/settings";
import { useToast } from "@/hooks/use-toast";

export function BackupSettings() {
  const { settings, isLoading, updateSettings, isUpdating } = useSettings();
  const { toast } = useToast();
  const [concurrencyLimit, setConcurrencyLimit] = useState<string>("");

  // Initialize from settings when loaded
  useEffect(() => {
    if (settings) {
      setConcurrencyLimit(settings.backup_concurrency_limit?.toString() || "3");
    }
  }, [settings]);

  if (isLoading) {
    return (
      <Card>
        <CardHeader>
          <Skeleton className="h-6 w-48" />
          <Skeleton className="h-4 w-64 mt-2" />
        </CardHeader>
        <CardContent className="space-y-6">
          <div className="flex items-center justify-between p-4 rounded-lg border">
            <div className="flex items-center gap-3">
              <Skeleton className="h-10 w-10 rounded-md" />
              <div>
                <Skeleton className="h-5 w-32" />
                <Skeleton className="h-4 w-48 mt-1" />
              </div>
            </div>
            <Skeleton className="h-10 w-24" />
          </div>
        </CardContent>
      </Card>
    );
  }

  const handleSave = async () => {
    const limit = parseInt(concurrencyLimit);
    if (isNaN(limit) || limit < 1 || limit > 20) {
      toast({
        title: "Invalid value",
        description: "Concurrency limit must be between 1 and 20",
        variant: "destructive",
      });
      return;
    }

    try {
      await updateSettings({ backup_concurrency_limit: limit });
      toast({
        title: "Success",
        description: "Backup concurrency limit updated successfully",
      });
    } catch (error: any) {
      toast({
        title: "Error",
        description: error.message || "Failed to update concurrency limit",
        variant: "destructive",
      });
    }
  };

  const hasChanges = settings?.backup_concurrency_limit?.toString() !== concurrencyLimit;

  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-lg">Backup Settings</CardTitle>
        <CardDescription>Configure backup execution behavior</CardDescription>
      </CardHeader>
      <CardContent className="space-y-6">
        <div className="flex items-start gap-3 p-4 rounded-lg border bg-blue-50 dark:bg-blue-950/20 border-blue-200 dark:border-blue-900">
          <Info className="w-5 h-5 text-blue-600 dark:text-blue-400 mt-0.5 flex-shrink-0" />
          <div className="space-y-1">
            <p className="text-sm font-medium text-blue-900 dark:text-blue-100">
              Concurrent Backup Execution
            </p>
            <p className="text-xs text-blue-700 dark:text-blue-300">
              Control how many backups can run simultaneously. Higher values allow more parallel backups but may consume more system resources.
            </p>
          </div>
        </div>

        <div className="space-y-3">
          <div className="flex items-center justify-between p-4 rounded-lg border bg-background/50">
            <div className="flex items-center gap-3 flex-1">
              <div className="p-2 rounded-md bg-primary/10">
                <Database className="w-4 h-4 text-primary" />
              </div>
              <div className="flex-1">
                <Label htmlFor="concurrency-limit" className="text-sm font-medium">
                  Maximum Concurrent Backups
                </Label>
                <p className="text-xs text-muted-foreground mt-1">
                  Number of backups that can run in parallel (1-20)
                </p>
              </div>
            </div>
            <div className="flex items-center gap-2">
              <Input
                id="concurrency-limit"
                type="number"
                min="1"
                max="20"
                value={concurrencyLimit}
                onChange={(e) => setConcurrencyLimit(e.target.value)}
                disabled={isUpdating}
                className="w-24"
              />
              {hasChanges && (
                <Button
                  onClick={handleSave}
                  disabled={isUpdating}
                  size="sm"
                >
                  {isUpdating ? "Saving..." : "Save"}
                </Button>
              )}
            </div>
          </div>
        </div>
      </CardContent>
    </Card>
  );
}

