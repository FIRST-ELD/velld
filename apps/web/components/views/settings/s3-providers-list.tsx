"use client";

import { useState } from "react";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { useS3Providers } from "@/hooks/use-s3-providers";
import { Plus, Edit, Trash2, CheckCircle2, Cloud, Loader2 } from "lucide-react";
import { S3ProviderDialog } from "./s3-provider-dialog";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Skeleton } from "@/components/ui/skeleton";

export function S3ProvidersList() {
  const { providers = [], isLoading, deleteProvider, setDefaultProvider, testProvider, isDeleting, isTesting } = useS3Providers();
  const [editingProvider, setEditingProvider] = useState<string | null>(null);
  const [isCreateDialogOpen, setIsCreateDialogOpen] = useState(false);
  const [deletingId, setDeletingId] = useState<string | null>(null);
  const [testingId, setTestingId] = useState<string | null>(null);

  const handleDelete = (id: string) => {
    setDeletingId(id);
  };

  const confirmDelete = () => {
    if (deletingId) {
      deleteProvider(deletingId);
      setDeletingId(null);
    }
  };

  const handleTest = async (id: string) => {
    setTestingId(id);
    testProvider(id);
    setTimeout(() => setTestingId(null), 2000);
  };

  if (isLoading) {
    return (
      <Card>
        <CardHeader>
          <CardTitle>S3 Storage Providers</CardTitle>
          <CardDescription>Manage your S3-compatible storage providers</CardDescription>
        </CardHeader>
        <CardContent>
          <div className="space-y-4">
            {[1, 2, 3].map((i) => (
              <Skeleton key={i} className="h-24 w-full" />
            ))}
          </div>
        </CardContent>
      </Card>
    );
  }

  return (
    <>
      <Card>
        <CardHeader>
          <div className="flex items-center justify-between">
            <div>
              <CardTitle>S3 Storage Providers</CardTitle>
              <CardDescription>Manage multiple S3-compatible storage providers for your backups</CardDescription>
            </div>
            <Button onClick={() => setIsCreateDialogOpen(true)}>
              <Plus className="mr-2 h-4 w-4" />
              Add Provider
            </Button>
          </div>
        </CardHeader>
        <CardContent>
          {!providers || providers.length === 0 ? (
            <div className="text-center py-8 text-muted-foreground">
              <Cloud className="mx-auto h-12 w-12 mb-4 opacity-50" />
              <p className="text-lg font-medium">No S3 providers configured</p>
              <p className="text-sm">Add your first S3 provider to start storing backups in the cloud</p>
            </div>
          ) : (
            <div className="space-y-4">
              {providers.map((provider) => (
                <div
                  key={provider.id}
                  className="border rounded-lg p-4 hover:bg-accent/50 transition-colors"
                >
                  <div className="flex items-start justify-between">
                    <div className="flex-1">
                      <div className="flex items-center gap-2 mb-2">
                        <h3 className="font-semibold">{provider.name}</h3>
                        {provider.is_default && (
                          <Badge variant="default" className="text-xs">
                            Default
                          </Badge>
                        )}
                      </div>
                      <div className="space-y-1 text-sm text-muted-foreground">
                        <p>
                          <span className="font-medium">Endpoint:</span> {provider.endpoint}
                        </p>
                        <p>
                          <span className="font-medium">Bucket:</span> {provider.bucket}
                        </p>
                        {provider.region && (
                          <p>
                            <span className="font-medium">Region:</span> {provider.region}
                          </p>
                        )}
                        <p>
                          <span className="font-medium">SSL:</span> {provider.use_ssl ? "Enabled" : "Disabled"}
                        </p>
                      </div>
                    </div>
                    <div className="flex items-center gap-2 ml-4">
                      <Button
                        variant="outline"
                        size="sm"
                        onClick={() => handleTest(provider.id)}
                        disabled={testingId === provider.id || isTesting}
                      >
                        {testingId === provider.id ? (
                          <Loader2 className="h-4 w-4 animate-spin" />
                        ) : (
                          <CheckCircle2 className="h-4 w-4" />
                        )}
                      </Button>
                      {!provider.is_default && (
                        <Button
                          variant="outline"
                          size="sm"
                          onClick={() => setDefaultProvider(provider.id)}
                        >
                          Set Default
                        </Button>
                      )}
                      <Button
                        variant="outline"
                        size="sm"
                        onClick={() => setEditingProvider(provider.id)}
                      >
                        <Edit className="h-4 w-4" />
                      </Button>
                      <Button
                        variant="outline"
                        size="sm"
                        onClick={() => handleDelete(provider.id)}
                        disabled={isDeleting}
                      >
                        <Trash2 className="h-4 w-4" />
                      </Button>
                    </div>
                  </div>
                </div>
              ))}
            </div>
          )}
        </CardContent>
      </Card>

      <S3ProviderDialog
        open={isCreateDialogOpen}
        onOpenChange={setIsCreateDialogOpen}
        provider={null}
      />

      {editingProvider && providers && providers.length > 0 && (
        <S3ProviderDialog
          open={!!editingProvider}
          onOpenChange={(open) => !open && setEditingProvider(null)}
          provider={providers.find((p) => p.id === editingProvider) || null}
        />
      )}

      <Dialog open={!!deletingId} onOpenChange={(open) => !open && setDeletingId(null)}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Delete S3 Provider</DialogTitle>
            <DialogDescription>
              Are you sure you want to delete this S3 provider? This action cannot be undone.
            </DialogDescription>
          </DialogHeader>
          <DialogFooter>
            <Button variant="outline" onClick={() => setDeletingId(null)}>
              Cancel
            </Button>
            <Button onClick={confirmDelete} variant="destructive">
              Delete
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </>
  );
}

