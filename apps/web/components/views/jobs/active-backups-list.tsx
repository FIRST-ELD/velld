"use client";

import { useQuery } from "@tanstack/react-query";
import { getActiveBackups, getBackups } from "@/lib/api/backups";
import { Card } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Database, Clock, Play, Terminal, History } from "lucide-react";
import { BackupJobViewer } from "@/components/views/backup/backup-job-viewer";
import { useState } from "react";
import { formatDistanceToNow } from "date-fns";
import { EmptyState } from "@/components/ui/empty-state";
import { Skeleton } from "@/components/ui/skeleton";
import { statusColors } from "@/types/base";
import { formatBackupStatus } from "@/lib/helper";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import type { BackupList } from "@/types/backup";

export function ActiveBackupsList() {
  const [viewingBackupId, setViewingBackupId] = useState<string | null>(null);
  const [activeTab, setActiveTab] = useState<'active' | 'all'>('active');

  const { data: activeData, isLoading: isLoadingActive, error: activeError } = useQuery({
    queryKey: ['active-backups'],
    queryFn: () => getActiveBackups(),
    refetchInterval: 5000, // Poll every 5 seconds for active backups
    refetchIntervalInBackground: true,
  });

  const { data: allBackupsData, isLoading: isLoadingAll, error: allError } = useQuery({
    queryKey: ['backups', { page: 1, limit: 50 }],
    queryFn: () => getBackups({ page: 1, limit: 50 }),
    enabled: activeTab === 'all',
    refetchInterval: activeTab === 'all' ? 10000 : false, // Poll every 10 seconds when viewing all
  });

  const activeBackups = activeData?.data || [];
  const allBackups = allBackupsData?.data || [];
  
  const isLoading = activeTab === 'active' ? isLoadingActive : isLoadingAll;
  const error = activeTab === 'active' ? activeError : allError;
  const backups = activeTab === 'active' ? activeBackups : allBackups;

  if (isLoading) {
    return (
      <Card className="p-6">
        <div className="space-y-4">
          <Skeleton className="h-10 w-64" />
          <Skeleton className="h-32 w-full" />
          <Skeleton className="h-32 w-full" />
        </div>
      </Card>
    );
  }

  if (error) {
    return (
      <Card className="p-6">
        <div className="text-center text-destructive">
          Failed to load active backups: {error instanceof Error ? error.message : "Unknown error"}
        </div>
      </Card>
    );
  }

  return (
    <>
      <Card className="p-6">
        <div className="space-y-4">
          <div className="flex items-center justify-between">
            <h2 className="text-lg font-semibold">Backup Jobs</h2>
            <Badge variant="outline" className="bg-primary/10">
              {activeTab === 'active' 
                ? `${activeBackups.length} ${activeBackups.length === 1 ? 'job' : 'jobs'} running`
                : `${allBackups.length} ${allBackups.length === 1 ? 'backup' : 'backups'}`
              }
            </Badge>
          </div>

          <Tabs value={activeTab} onValueChange={(v) => setActiveTab(v as 'active' | 'all')}>
            <TabsList>
              <TabsTrigger value="active">
                <Play className="h-4 w-4 mr-2" />
                Active ({activeBackups.length})
              </TabsTrigger>
              <TabsTrigger value="all">
                <History className="h-4 w-4 mr-2" />
                All Jobs ({allBackups.length})
              </TabsTrigger>
            </TabsList>

            <TabsContent value="active" className="mt-4">
              {activeBackups.length === 0 ? (
                <EmptyState
                  icon={Play}
                  title="No active backups"
                  description="There are no backup jobs currently running. Start a backup from the connections page to see it here."
                  variant="minimal"
                />
              ) : (
                <div className="space-y-3">
                  {activeBackups.map((backup: BackupList) => {
                    const startedTime = backup.started_time ? new Date(backup.started_time) : null;
                    const timeAgo = startedTime ? formatDistanceToNow(startedTime, { addSuffix: true }) : null;

                    return (
                      <div
                        key={backup.id}
                        className="p-4 rounded-lg border bg-card hover:bg-accent/50 transition-colors"
                      >
                        <div className="flex items-start justify-between gap-4">
                          <div className="flex items-start gap-4 flex-1">
                            <div className="p-2.5 rounded-md bg-primary/10 shrink-0">
                              <Database className="h-5 w-5 text-primary" />
                            </div>
                            <div className="flex-1 min-w-0">
                              <div className="flex items-center gap-2 mb-1">
                                <h3 className="font-medium text-sm">{backup.database_name}</h3>
                                <Badge 
                                  variant="outline" 
                                  className={statusColors[backup.status as keyof typeof statusColors] || "bg-gray-500/15 text-gray-500 border-gray-500/20"}
                                >
                                  {formatBackupStatus(backup.status)}
                                </Badge>
                                <Badge variant="outline" className="text-xs">
                                  {backup.database_type}
                                </Badge>
                              </div>
                              <div className="flex items-center gap-4 text-xs text-muted-foreground mt-2">
                                {timeAgo && (
                                  <div className="flex items-center gap-1">
                                    <Clock className="h-3 w-3" />
                                    Started {timeAgo}
                                  </div>
                                )}
                                <div className="text-xs">
                                  ID: {backup.id.slice(0, 8)}...
                                </div>
                              </div>
                            </div>
                          </div>
                          <Button
                            variant="outline"
                            size="sm"
                            onClick={() => setViewingBackupId(backup.id)}
                            className="gap-2"
                          >
                            <Terminal className="h-4 w-4" />
                            View Logs
                          </Button>
                        </div>
                      </div>
                    );
                  })}
                </div>
              )}
            </TabsContent>

            <TabsContent value="all" className="mt-4">
              {allBackups.length === 0 ? (
                <EmptyState
                  icon={History}
                  title="No backups"
                  description="No backup jobs found. Start a backup from the connections page."
                  variant="minimal"
                />
              ) : (
                <div className="space-y-3">
                  {allBackups.map((backup: BackupList) => {
                    const startedTime = backup.started_time ? new Date(backup.started_time) : null;
                    const timeAgo = startedTime ? formatDistanceToNow(startedTime, { addSuffix: true }) : null;

                    return (
                      <div
                        key={backup.id}
                        className="p-4 rounded-lg border bg-card hover:bg-accent/50 transition-colors"
                      >
                        <div className="flex items-start justify-between gap-4">
                          <div className="flex items-start gap-4 flex-1">
                            <div className="p-2.5 rounded-md bg-primary/10 shrink-0">
                              <Database className="h-5 w-5 text-primary" />
                            </div>
                            <div className="flex-1 min-w-0">
                              <div className="flex items-center gap-2 mb-1">
                                <h3 className="font-medium text-sm">{backup.database_name}</h3>
                                <Badge 
                                  variant="outline" 
                                  className={statusColors[backup.status as keyof typeof statusColors] || "bg-gray-500/15 text-gray-500 border-gray-500/20"}
                                >
                                  {formatBackupStatus(backup.status)}
                                </Badge>
                                <Badge variant="outline" className="text-xs">
                                  {backup.database_type}
                                </Badge>
                              </div>
                              <div className="flex items-center gap-4 text-xs text-muted-foreground mt-2">
                                {timeAgo && (
                                  <div className="flex items-center gap-1">
                                    <Clock className="h-3 w-3" />
                                    {backup.completed_time ? 'Completed' : 'Started'} {timeAgo}
                                  </div>
                                )}
                                <div className="text-xs">
                                  ID: {backup.id.slice(0, 8)}...
                                </div>
                              </div>
                            </div>
                          </div>
                          <Button
                            variant="outline"
                            size="sm"
                            onClick={() => setViewingBackupId(backup.id)}
                            className="gap-2"
                          >
                            <Terminal className="h-4 w-4" />
                            View Logs
                          </Button>
                        </div>
                      </div>
                    );
                  })}
                </div>
              )}
            </TabsContent>
          </Tabs>
        </div>
      </Card>

      {viewingBackupId && (
        <BackupJobViewer
          backupId={viewingBackupId}
          open={!!viewingBackupId}
          onOpenChange={(open) => !open && setViewingBackupId(null)}
        />
      )}
    </>
  );
}

