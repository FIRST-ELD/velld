"use client";

import { Sidebar } from "@/components/layout/sidebar";
import { ProfileSettings } from "@/components/views/settings/profile-settings";
import { S3ProvidersList } from "@/components/views/settings/s3-providers-list";
import { BackupSettings } from "@/components/views/settings/backup-settings";
import { SignOutCard } from "@/components/views/settings/sign-out-card";


export default function SettingsPage() {
  return (
    <div className="flex h-screen bg-background">
      <Sidebar />
      <div className="flex-1 overflow-auto lg:ml-64">
        <div className="px-4 sm:px-6 lg:px-8 py-4 sm:py-6 space-y-4 sm:space-y-6 pt-16 lg:pt-6">
          <ProfileSettings />
          <BackupSettings />
          <S3ProvidersList />
          <SignOutCard />
        </div>
      </div>
    </div>
  );
}