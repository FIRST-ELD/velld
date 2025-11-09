import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { useToast } from "@/hooks/use-toast";
import {
  listS3Providers,
  createS3Provider,
  updateS3Provider,
  deleteS3Provider,
  setDefaultS3Provider,
  testS3Provider,
  type S3Provider,
  type S3ProviderRequest,
} from "@/lib/api/s3-providers";

export function useS3Providers() {
  const { toast } = useToast();
  const queryClient = useQueryClient();

  const { data: providersData, isLoading, error } = useQuery({
    queryKey: ["s3-providers"],
    queryFn: listS3Providers,
  });

  const providers = providersData || [];

  const createMutation = useMutation({
    mutationFn: (provider: S3ProviderRequest) => createS3Provider(provider),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["s3-providers"] });
      toast({
        title: "Success",
        description: "S3 provider created successfully",
      });
    },
    onError: (error: Error) => {
      toast({
        title: "Error",
        description: error.message || "Failed to create S3 provider",
        variant: "destructive",
      });
    },
  });

  const updateMutation = useMutation({
    mutationFn: ({ id, provider }: { id: string; provider: S3ProviderRequest }) =>
      updateS3Provider(id, provider),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["s3-providers"] });
      toast({
        title: "Success",
        description: "S3 provider updated successfully",
      });
    },
    onError: (error: Error) => {
      toast({
        title: "Error",
        description: error.message || "Failed to update S3 provider",
        variant: "destructive",
      });
    },
  });

  const deleteMutation = useMutation({
    mutationFn: (id: string) => deleteS3Provider(id),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["s3-providers"] });
      toast({
        title: "Success",
        description: "S3 provider deleted successfully",
      });
    },
    onError: (error: Error) => {
      toast({
        title: "Error",
        description: error.message || "Failed to delete S3 provider",
        variant: "destructive",
      });
    },
  });

  const setDefaultMutation = useMutation({
    mutationFn: (id: string) => setDefaultS3Provider(id),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["s3-providers"] });
      toast({
        title: "Success",
        description: "Default S3 provider updated successfully",
      });
    },
    onError: (error: Error) => {
      toast({
        title: "Error",
        description: error.message || "Failed to set default provider",
        variant: "destructive",
      });
    },
  });

  const testMutation = useMutation({
    mutationFn: (id: string) => testS3Provider(id),
    onSuccess: () => {
      toast({
        title: "Connection Successful",
        description: "Successfully connected to S3 provider",
      });
    },
    onError: (error: Error) => {
      toast({
        title: "Connection Failed",
        description: error.message || "Failed to connect to S3 provider",
        variant: "destructive",
      });
    },
  });

  return {
    providers,
    isLoading,
    error,
    createProvider: createMutation.mutate,
    updateProvider: updateMutation.mutate,
    deleteProvider: deleteMutation.mutate,
    setDefaultProvider: setDefaultMutation.mutate,
    testProvider: testMutation.mutate,
    isCreating: createMutation.isPending,
    isUpdating: updateMutation.isPending,
    isDeleting: deleteMutation.isPending,
    isSettingDefault: setDefaultMutation.isPending,
    isTesting: testMutation.isPending,
  };
}

