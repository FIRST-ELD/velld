"use client";

import { Sidebar } from "@/components/layout/sidebar";
import { PageHeader } from "@/components/layout/page-header";
import { ActiveBackupsList } from "@/components/views/jobs/active-backups-list";

export default function JobsPage() {
  return (
    <div className="flex h-screen bg-background">
      <Sidebar />
      <div className="flex-1 overflow-auto lg:ml-64">
        <div className="h-full px-4 sm:px-6 lg:px-8 py-4 sm:py-6 space-y-4 sm:space-y-6 pt-16 lg:pt-6">
          <PageHeader
            title="Backup Jobs"
            description="Monitor all running backup operations"
          />

          <ActiveBackupsList />
        </div>
      </div>
    </div>
  );
}

