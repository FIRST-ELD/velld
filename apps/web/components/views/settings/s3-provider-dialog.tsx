"use client";

import { useState, useEffect } from "react";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Switch } from "@/components/ui/switch";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { useS3Providers } from "@/hooks/use-s3-providers";
import { Loader2, HelpCircle } from "lucide-react";
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from "@/components/ui/tooltip";
import type { S3Provider } from "@/lib/api/s3-providers";

const S3_PROVIDERS = [
  { value: "aws", label: "AWS S3", endpoint: "s3.amazonaws.com", region: "us-east-1", ssl: true },
  { value: "minio", label: "MinIO", endpoint: "localhost:9000", region: "us-east-1", ssl: false },
  { value: "backblaze", label: "Backblaze B2", endpoint: "s3.us-east-005.backblazeb2.com", region: "us-east-005", ssl: true },
  { value: "scaleway", label: "Scaleway", endpoint: "s3.fr-par.scw.cloud", region: "fr-par", ssl: true },
  { value: "storj", label: "Storj DCS", endpoint: "gateway.storjshare.io", region: "global", ssl: true },
  { value: "digitalocean", label: "DigitalOcean Spaces", endpoint: "nyc3.digitaloceanspaces.com", region: "nyc3", ssl: true },
  { value: "wasabi", label: "Wasabi", endpoint: "s3.wasabisys.com", region: "us-east-1", ssl: true },
  { value: "custom", label: "Custom / Other", endpoint: "", region: "", ssl: true },
];

interface S3ProviderDialogProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  provider: S3Provider | null;
}

export function S3ProviderDialog({ open, onOpenChange, provider }: S3ProviderDialogProps) {
  const { createProvider, updateProvider, isCreating, isUpdating } = useS3Providers();
  const isEditing = !!provider;

  const [formData, setFormData] = useState({
    name: "",
    endpoint: "",
    region: "",
    bucket: "",
    access_key: "",
    secret_key: "",
    use_ssl: true,
    path_prefix: "",
    is_default: false,
  });

  useEffect(() => {
    if (provider) {
      setFormData({
        name: provider.name,
        endpoint: provider.endpoint,
        region: provider.region || "",
        bucket: provider.bucket,
        access_key: "", // Don't show existing access key for security
        secret_key: "", // Don't show existing secret key for security
        use_ssl: provider.use_ssl,
        path_prefix: provider.path_prefix || "",
        is_default: provider.is_default,
      });
    } else {
      setFormData({
        name: "",
        endpoint: "",
        region: "",
        bucket: "",
        access_key: "",
        secret_key: "",
        use_ssl: true,
        path_prefix: "",
        is_default: false,
      });
    }
  }, [provider, open]);

  const handleProviderChange = (providerValue: string) => {
    const providerConfig = S3_PROVIDERS.find((p) => p.value === providerValue);
    if (providerConfig && providerValue !== "custom") {
      setFormData((prev) => ({
        ...prev,
        endpoint: providerConfig.endpoint,
        region: providerConfig.region,
        use_ssl: providerConfig.ssl,
      }));
    }
  };

  const handleSubmit = async () => {
    if (!formData.name || !formData.endpoint || !formData.bucket || !formData.access_key) {
      return;
    }

    // For updates, only send secret_key if it was changed
    let dataToSave: typeof formData = { ...formData };
    if (isEditing && !dataToSave.secret_key) {
      // Don't send secret_key if it's empty (means user didn't change it)
      // eslint-disable-next-line @typescript-eslint/no-unused-vars
      const { secret_key, ...rest } = dataToSave;
      dataToSave = rest as typeof formData;
    }

    if (isEditing && provider) {
      updateProvider({ id: provider.id, provider: dataToSave });
    } else {
      // For new providers, secret_key is required
      if (!dataToSave.secret_key) {
        return;
      }
      createProvider(dataToSave);
    }

    onOpenChange(false);
  };

  const isLoading = isCreating || isUpdating;

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-2xl max-h-[90vh] overflow-y-auto">
        <DialogHeader>
          <DialogTitle>{isEditing ? "Edit S3 Provider" : "Add S3 Provider"}</DialogTitle>
          <DialogDescription>
            {isEditing
              ? "Update your S3 provider configuration. Leave secret key empty to keep the existing one."
              : "Configure a new S3-compatible storage provider for your backups"}
          </DialogDescription>
        </DialogHeader>

        <div className="space-y-4 py-4">
          {/* Provider Name */}
          <div className="space-y-2">
            <Label htmlFor="name">
              Provider Name <span className="text-destructive">*</span>
            </Label>
            <Input
              id="name"
              placeholder="My S3 Provider"
              value={formData.name}
              onChange={(e) => setFormData({ ...formData, name: e.target.value })}
            />
          </div>

          {/* Provider Preset */}
          <div className="space-y-2">
            <Label htmlFor="provider">
              Provider Preset
              <TooltipProvider>
                <Tooltip>
                  <TooltipTrigger asChild>
                    <HelpCircle className="inline w-3.5 h-3.5 ml-1.5 text-muted-foreground" />
                  </TooltipTrigger>
                  <TooltipContent>
                    <p>Select a provider to auto-fill endpoint and region settings</p>
                  </TooltipContent>
                </Tooltip>
              </TooltipProvider>
            </Label>
            <Select onValueChange={handleProviderChange}>
              <SelectTrigger>
                <SelectValue placeholder="Select a provider or use custom" />
              </SelectTrigger>
              <SelectContent>
                {S3_PROVIDERS.map((p) => (
                  <SelectItem key={p.value} value={p.value}>
                    {p.label}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>

          {/* Grid Layout for Endpoint and Region */}
          <div className="grid grid-cols-2 gap-4">
            <div className="space-y-2">
              <Label htmlFor="endpoint">
                Endpoint <span className="text-destructive">*</span>
                <TooltipProvider>
                  <Tooltip>
                    <TooltipTrigger asChild>
                      <HelpCircle className="inline w-3.5 h-3.5 ml-1.5 text-muted-foreground" />
                    </TooltipTrigger>
                    <TooltipContent>
                      <p>S3 API endpoint URL (without https://)</p>
                    </TooltipContent>
                  </Tooltip>
                </TooltipProvider>
              </Label>
              <Input
                id="endpoint"
                placeholder="s3.amazonaws.com"
                value={formData.endpoint}
                onChange={(e) => setFormData({ ...formData, endpoint: e.target.value })}
              />
            </div>

            <div className="space-y-2">
              <Label htmlFor="region">
                Region
                <TooltipProvider>
                  <Tooltip>
                    <TooltipTrigger asChild>
                      <HelpCircle className="inline w-3.5 h-3.5 ml-1.5 text-muted-foreground" />
                    </TooltipTrigger>
                    <TooltipContent>
                      <p>S3 region (e.g., us-east-1)</p>
                    </TooltipContent>
                  </Tooltip>
                </TooltipProvider>
              </Label>
              <Input
                id="region"
                placeholder="us-east-1"
                value={formData.region}
                onChange={(e) => setFormData({ ...formData, region: e.target.value })}
              />
            </div>
          </div>

          {/* Bucket Name */}
          <div className="space-y-2">
            <Label htmlFor="bucket">
              Bucket Name <span className="text-destructive">*</span>
            </Label>
            <Input
              id="bucket"
              placeholder="velld-backups"
              value={formData.bucket}
              onChange={(e) => setFormData({ ...formData, bucket: e.target.value })}
            />
          </div>

          {/* Access Key and Secret Key */}
          <div className="grid grid-cols-2 gap-4">
            <div className="space-y-2">
              <Label htmlFor="access-key">
                Access Key ID <span className="text-destructive">*</span>
              </Label>
              <Input
                id="access-key"
                placeholder="AKIAIOSFODNN7EXAMPLE"
                value={formData.access_key}
                onChange={(e) => setFormData({ ...formData, access_key: e.target.value })}
              />
            </div>

            <div className="space-y-2">
              <Label htmlFor="secret-key">
                Secret Access Key {!isEditing && <span className="text-destructive">*</span>}
                {isEditing && (
                  <span className="text-xs text-muted-foreground ml-1">(leave empty to keep existing)</span>
                )}
              </Label>
              <Input
                id="secret-key"
                type="password"
                placeholder={isEditing ? "Leave empty to keep existing" : "wJalrXUtnFEMI/K7MDENG..."}
                value={formData.secret_key}
                onChange={(e) => setFormData({ ...formData, secret_key: e.target.value })}
              />
            </div>
          </div>

          {/* Path Prefix */}
          <div className="space-y-2">
            <Label htmlFor="path-prefix">
              Path Prefix (Optional)
              <TooltipProvider>
                <Tooltip>
                  <TooltipTrigger asChild>
                    <HelpCircle className="inline w-3.5 h-3.5 ml-1.5 text-muted-foreground" />
                  </TooltipTrigger>
                  <TooltipContent>
                    <p>Folder path inside bucket (e.g., backups/production)</p>
                  </TooltipContent>
                </Tooltip>
              </TooltipProvider>
            </Label>
            <Input
              id="path-prefix"
              placeholder="backups/production"
              value={formData.path_prefix}
              onChange={(e) => setFormData({ ...formData, path_prefix: e.target.value })}
            />
          </div>

          {/* SSL and Default Toggle */}
          <div className="flex items-center justify-between space-x-4">
            <div className="flex items-center space-x-2">
              <Switch
                id="use-ssl"
                checked={formData.use_ssl}
                onCheckedChange={(checked) => setFormData({ ...formData, use_ssl: checked })}
              />
              <Label htmlFor="use-ssl">Use SSL</Label>
            </div>
            <div className="flex items-center space-x-2">
              <Switch
                id="is-default"
                checked={formData.is_default}
                onCheckedChange={(checked) => setFormData({ ...formData, is_default: checked })}
              />
              <Label htmlFor="is-default">Set as Default</Label>
            </div>
          </div>
        </div>

        <DialogFooter>
          <Button variant="outline" onClick={() => onOpenChange(false)} disabled={isLoading}>
            Cancel
          </Button>
          <Button onClick={handleSubmit} disabled={isLoading || !formData.name || !formData.endpoint || !formData.bucket || !formData.access_key || (!isEditing && !formData.secret_key)}>
            {isLoading && <Loader2 className="mr-2 h-4 w-4 animate-spin" />}
            {isEditing ? "Update" : "Create"}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}

