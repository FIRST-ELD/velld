'use client';

import { Clock, Calendar } from 'lucide-react';
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Switch } from "@/components/ui/switch";
import { Label } from '@/components/ui/label';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import type { Connection } from '@/types/connection';
import { useBackup } from '@/hooks/use-backup';
import { useState, useEffect } from 'react';
import { getScheduleFrequency } from '@/lib/helper';

const CRON_SCHEDULES = {
  'test': '0 */1 * * * *',  // Run every 1 minutes
  'hourly': '0 0 * * * *',  // Every 1 hour
  'every3hours': '0 0 */3 * * *',  // Every 3 hours
  'every6hours': '0 0 */6 * * *',  // Every 6 hours
  'daily': '0 0 0 * * *',
  'weekly': '0 0 0 * * 0',
  'monthly': '0 0 0 1 * *'
};

const RETENTION_DAYS = {
  '7': 7,
  '30': 30,
  '90': 90,
  '365': 365
};

interface BackupScheduleDialogProps {
  connectionId: string | null;
  connection?: Connection;
  onClose: () => void;
}

export function BackupScheduleDialog({
  connectionId,
  connection,
  onClose,
}: BackupScheduleDialogProps) {
  const { createSchedule, updateExistingSchedule, disableSchedule, isScheduling, isDisabling, isUpdating } = useBackup();
  const [enabled, setEnabled] = useState(false);

  const getRetentionFromDays = (days?: number | null) => {
    if (!days) return '30';
    return days.toString();
  };

  const [schedule, setSchedule] = useState(getScheduleFrequency(connection?.cron_schedule) || 'daily');
  const [retention, setRetention] = useState(getRetentionFromDays(connection?.retention_days));

  useEffect(() => {
    if (connection) {
      setEnabled(connection.backup_enabled);
      setSchedule(getScheduleFrequency(connection.cron_schedule) || 'daily');
      setRetention(getRetentionFromDays(connection.retention_days));
    }
  }, [connection]);

  if (!connectionId || !connection) return null;

  const handleScheduleChange = async (checked: boolean) => {
    try {
      if (checked) {
        await createSchedule({
          connection_id: connectionId,
          cron_schedule: CRON_SCHEDULES[schedule as keyof typeof CRON_SCHEDULES],
          retention_days: RETENTION_DAYS[retention as keyof typeof RETENTION_DAYS]
        });
        setEnabled(true);
      } else {
        await disableSchedule(connectionId);
        setEnabled(false);
      }
    } catch (error) {
      // If there's an error, revert the switch state
      setEnabled(!checked);
      console.error('Failed to update schedule:', error);
    }
  };

  const handleScheduleSubmit = async (newSchedule: string, newRetention: string) => {
    try {
      if (enabled) {
        // Update existing schedule
        await updateExistingSchedule({
          connectionId,
          params: {
            cron_schedule: CRON_SCHEDULES[newSchedule as keyof typeof CRON_SCHEDULES],
            retention_days: RETENTION_DAYS[newRetention as keyof typeof RETENTION_DAYS]
          }
        });
      } else {
        // Create new schedule
        await createSchedule({
          connection_id: connectionId,
          cron_schedule: CRON_SCHEDULES[newSchedule as keyof typeof CRON_SCHEDULES],
          retention_days: RETENTION_DAYS[newRetention as keyof typeof RETENTION_DAYS]
        });
        setEnabled(true);
      }
      setSchedule(newSchedule);
      setRetention(newRetention);
    } catch (error) {
      console.error('Failed to update schedule:', error);
    }
  };

  return (
    <Dialog open={!!connectionId} onOpenChange={() => onClose()}>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>Backup Schedule - {connection.name}</DialogTitle>
        </DialogHeader>
        <div className="space-y-6 py-4">
          <div className="flex items-center justify-between space-x-4">
            <div className="space-y-0.5">
              <Label>Automatic Backups</Label>
              <div className="text-sm text-muted-foreground">
                Enable scheduled backups for this database
              </div>
            </div>
            <Switch
              checked={enabled}
              disabled={isScheduling || isDisabling || isUpdating}
              onCheckedChange={handleScheduleChange}
            />
          </div>

          {enabled && (
            <>
              <div className="space-y-2">
                <Label className="text-sm flex items-center">
                  <Clock className="h-4 w-4 mr-1" />
                  Backup Frequency
                </Label>
                <Select
                  value={schedule}
                  onValueChange={(value) => handleScheduleSubmit(value, retention)}
                  disabled={isScheduling}
                >
                  <SelectTrigger>
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="test">Every Minute (Test)</SelectItem>
                    <SelectItem value="hourly">Every 1 Hour</SelectItem>
                    <SelectItem value="every3hours">Every 3 Hours</SelectItem>
                    <SelectItem value="every6hours">Every 6 Hours</SelectItem>
                    <SelectItem value="daily">Daily</SelectItem>
                    <SelectItem value="weekly">Weekly</SelectItem>
                    <SelectItem value="monthly">Monthly</SelectItem>
                  </SelectContent>
                </Select>
              </div>

              <div className="space-y-2">
                <Label className="text-sm flex items-center">
                  <Calendar className="h-4 w-4 mr-1" />
                  Retention Period
                </Label>
                <Select
                  value={retention}
                  onValueChange={(value) => handleScheduleSubmit(schedule, value)}
                  disabled={isScheduling}
                >
                  <SelectTrigger>
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="7">7 Days</SelectItem>
                    <SelectItem value="30">30 Days</SelectItem>
                    <SelectItem value="90">90 Days</SelectItem>
                    <SelectItem value="365">1 Year</SelectItem>
                  </SelectContent>
                </Select>
              </div>
            </>
          )}
        </div>
      </DialogContent>
    </Dialog>
  );
}