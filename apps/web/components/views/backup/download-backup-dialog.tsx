"use client";

import { useState, useEffect } from "react";
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogDescription } from "@/components/ui/dialog";
import { Button } from "@/components/ui/button";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { useQuery, useMutation } from "@tanstack/react-query";
import { getBackupS3Providers, createShareableLink, downloadBackup, type BackupS3Provider, type ShareableLink } from "@/lib/api/backups";
import { useS3Providers } from "@/hooks/use-s3-providers";
import { useToast } from "@/hooks/use-toast";
import { Download, Link2, Copy } from "lucide-react";
import { Badge } from "@/components/ui/badge";

interface DownloadBackupDialogProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  backupId: string;
  backupPath: string;
}

export function DownloadBackupDialog({ open, onOpenChange, backupId, backupPath }: DownloadBackupDialogProps) {
  const [selectedProviderId, setSelectedProviderId] = useState<string>("");
  const [shareableLink, setShareableLink] = useState<ShareableLink | null>(null);
  const [expiresIn, setExpiresIn] = useState<number>(24);
  const { toast } = useToast();
  const { providers } = useS3Providers();

  // Debug log when dialog opens
  useEffect(() => {
    if (open) {
      console.log("DownloadBackupDialog opened", { backupId, backupPath, open });
    }
  }, [open, backupId, backupPath]);

  const { data: backupProviders, isLoading: isLoadingProviders } = useQuery({
    queryKey: ['backup-s3-providers', backupId],
    queryFn: () => getBackupS3Providers(backupId),
    enabled: open && !!backupId,
  });

  const { mutate: download, isPending: isDownloading } = useMutation({
    mutationFn: async (providerId?: string) => {
      console.log("Download mutation started", { backupId, providerId });
      try {
        console.log("Calling downloadBackup API...");
        const blob = await downloadBackup(backupId, providerId);
        console.log("Downloaded blob:", { size: blob.size, type: blob.type });
        if (!blob || blob.size === 0) {
          throw new Error("Downloaded file is empty");
        }
        const url = window.URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        const filename = backupPath.split('\\').pop()?.split('/').pop() || `backup-${backupId}.sql`;
        console.log("Triggering download with filename:", filename);
        a.download = filename;
        document.body.appendChild(a);
        a.click();
        // Small delay before cleanup to ensure download starts
        setTimeout(() => {
          window.URL.revokeObjectURL(url);
          a.remove();
        }, 100);
      } catch (error) {
        console.error("Download error:", error);
        throw error;
      }
    },
    onSuccess: () => {
      toast({
        title: "Success",
        description: "Backup downloaded successfully",
      });
      onOpenChange(false);
    },
    onError: (error: Error | unknown) => {
      console.error("Download mutation error:", error);
      const errorMessage = error instanceof Error ? error.message : "Failed to download backup";
      toast({
        title: "Error",
        description: errorMessage,
        variant: "destructive",
      });
    },
  });

  const { mutate: createLink, isPending: isCreatingLink } = useMutation({
    mutationFn: async () => {
      try {
        const providerId = selectedProviderId && selectedProviderId.trim() !== "" ? selectedProviderId : undefined;
        const link = await createShareableLink(backupId, providerId, expiresIn);
        setShareableLink(link);
        return link;
      } catch (error) {
        console.error("Create link error:", error);
        throw error;
      }
    },
    onSuccess: () => {
      toast({
        title: "Success",
        description: "Shareable link created successfully",
      });
    },
    onError: (error: Error | unknown) => {
      console.error("Create link mutation error:", error);
      const errorMessage = error instanceof Error ? error.message : "Failed to create shareable link";
      toast({
        title: "Error",
        description: errorMessage,
        variant: "destructive",
      });
    },
  });

  // Get provider names for display
  const getProviderName = (providerId: string) => {
    const provider = providers?.find(p => p.id === providerId);
    return provider?.name || providerId.slice(0, 8) + "...";
  };

  // Reset state when dialog opens/closes
  useEffect(() => {
    if (open) {
      setShareableLink(null);
      setSelectedProviderId("");
      setExpiresIn(24);
    }
  }, [open]);

  // Set default provider when providers load
  useEffect(() => {
    if (backupProviders && backupProviders.length > 0 && !selectedProviderId) {
      setSelectedProviderId(backupProviders[0].provider_id);
    }
  }, [backupProviders, selectedProviderId]);

  const handleCopyLink = () => {
    if (shareableLink) {
      const fullUrl = `${window.location.origin}${shareableLink.url}`;
      navigator.clipboard.writeText(fullUrl);
      toast({
        title: "Copied",
        description: "Shareable link copied to clipboard",
      });
    }
  };

  const availableProviders = backupProviders || [];

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-[500px]">
        <DialogHeader>
          <DialogTitle>Download Backup</DialogTitle>
          <DialogDescription>
            Choose an S3 provider to download from or create a shareable link
          </DialogDescription>
        </DialogHeader>

        <div className="space-y-6 py-4">
          {/* S3 Provider Selection */}
          {isLoadingProviders ? (
            <div className="text-sm text-muted-foreground">Loading providers...</div>
          ) : availableProviders.length > 0 ? (
            <div className="space-y-2">
              <Label htmlFor="provider">S3 Provider</Label>
              <Select value={selectedProviderId} onValueChange={setSelectedProviderId}>
                <SelectTrigger id="provider">
                  <SelectValue placeholder="Select S3 provider" />
                </SelectTrigger>
                <SelectContent>
                  {availableProviders.map((bp: BackupS3Provider) => (
                    <SelectItem key={bp.provider_id} value={bp.provider_id}>
                      {getProviderName(bp.provider_id)}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
              <p className="text-xs text-muted-foreground">
                {availableProviders.length} provider{availableProviders.length !== 1 ? 's' : ''} available
              </p>
            </div>
          ) : (
            <div className="text-sm text-muted-foreground">
              No S3 providers found for this backup. Downloading from default source.
            </div>
          )}

          {/* Download Button */}
          <div className="space-y-2">
            <Button
              type="button"
              onClick={async (e) => {
                e.preventDefault();
                e.stopPropagation();
                
                console.log("=== DOWNLOAD BUTTON CLICKED ===", { 
                  backupId, 
                  selectedProviderId, 
                  availableProviders,
                  isDownloading,
                  backupPath,
                  downloadMutation: typeof download,
                  downloadValue: download
                });
                
                if (!backupId) {
                  console.error("No backupId provided");
                  toast({
                    title: "Error",
                    description: "Backup ID is missing",
                    variant: "destructive",
                  });
                  return;
                }
                
                const providerId = selectedProviderId && selectedProviderId.trim() !== "" ? selectedProviderId : undefined;
                console.log("Provider ID to use:", providerId);
                
                // Show immediate feedback
                toast({
                  title: "Starting download...",
                  description: `Downloading backup ${backupId}`,
                });
                
                // Call the mutation
                console.log("Calling download mutation...");
                download(providerId);
              }}
              disabled={isDownloading || !backupId}
              className="w-full"
            >
              <Download className="h-4 w-4 mr-2" />
              {isDownloading ? "Downloading..." : "Download Backup"}
            </Button>
            {!backupId && (
              <p className="text-xs text-red-500">Warning: Backup ID is missing!</p>
            )}
          </div>

          {/* Divider */}
          <div className="relative">
            <div className="absolute inset-0 flex items-center">
              <span className="w-full border-t" />
            </div>
            <div className="relative flex justify-center text-xs uppercase">
              <span className="bg-background px-2 text-muted-foreground">Or</span>
            </div>
          </div>

          {/* Shareable Link Section */}
          <div className="space-y-4">
            <div className="space-y-2">
              <Label htmlFor="expires">Link Expires In (hours)</Label>
              <Input
                id="expires"
                type="number"
                min="1"
                max="168"
                value={expiresIn}
                onChange={(e) => setExpiresIn(parseInt(e.target.value) || 24)}
              />
              <p className="text-xs text-muted-foreground">
                Link will expire in {expiresIn} hour{expiresIn !== 1 ? 's' : ''} (max 7 days)
              </p>
            </div>

            <Button
              type="button"
              onClick={(e) => {
                e.preventDefault();
                e.stopPropagation();
                createLink();
              }}
              disabled={isCreatingLink}
              variant="outline"
              className="w-full"
            >
              <Link2 className="h-4 w-4 mr-2" />
              {isCreatingLink ? "Creating..." : "Create Shareable Link"}
            </Button>

            {/* Shareable Link Display */}
            {shareableLink && (
              <div className="space-y-2 p-4 rounded-lg border bg-muted/50">
                <div className="flex items-center justify-between">
                  <Label className="text-sm font-medium">Shareable Link</Label>
                  <Badge variant="outline" className="text-xs">
                    Expires: {new Date(shareableLink.expires_at).toLocaleString()}
                  </Badge>
                </div>
                <div className="flex items-center gap-2">
                  <Input
                    value={`${window.location.origin}${shareableLink.url}`}
                    readOnly
                    className="flex-1 font-mono text-xs"
                  />
                  <Button
                    type="button"
                    variant="outline"
                    size="icon"
                    onClick={(e) => {
                      e.preventDefault();
                      e.stopPropagation();
                      handleCopyLink();
                    }}
                  >
                    <Copy className="h-4 w-4" />
                  </Button>
                </div>
                <p className="text-xs text-muted-foreground">
                  Anyone with this link can download the backup. Share it securely.
                </p>
              </div>
            )}
          </div>
        </div>
      </DialogContent>
    </Dialog>
  );
}

