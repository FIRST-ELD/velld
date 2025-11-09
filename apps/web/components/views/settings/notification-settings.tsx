"use client";

import { useState, useEffect } from "react";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Switch } from "@/components/ui/switch";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Bell, Mail, Info, Send, Loader2, CheckCircle2 } from "lucide-react";
import { useSettings } from "@/hooks/use-settings";
import { Skeleton } from "@/components/ui/skeleton";
import { UpdateSettingsRequest } from "@/types/settings";
import { testTelegramConnection, getTelegramChats, TelegramChat, TelegramChatInfo } from "@/lib/api/settings";
import { useToast } from "@/hooks/use-toast";

type SettingsValue = string | number | boolean;

export function NotificationSettings() {
  const { settings, isLoading, updateSettings, isUpdating } = useSettings();
  const { toast } = useToast();
  const [formValues, setFormValues] = useState<UpdateSettingsRequest>({});
  const [telegramBotToken, setTelegramBotToken] = useState("");
  const [telegramChatID, setTelegramChatID] = useState("");
  const [availableChats, setAvailableChats] = useState<TelegramChat[]>([]);
  const [chatInfo, setChatInfo] = useState<TelegramChatInfo | null>(null);
  const [isLoadingChats, setIsLoadingChats] = useState(false);
  const [isTestingConnection, setIsTestingConnection] = useState(false);

  // Initialize from settings when loaded
  useEffect(() => {
    if (settings) {
      // Note: We can't read the bot token from settings (it's hidden for security)
      // User needs to enter it again or we'd need a separate "view token" feature
      if (settings.telegram_chat_id) {
        setTelegramChatID(settings.telegram_chat_id);
      }
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
          {[1, 2, 3].map((i) => (
            <div key={i} className="flex items-center justify-between p-4 rounded-lg border">
              <div className="flex items-center gap-3">
                <Skeleton className="h-10 w-10 rounded-md" />
                <div>
                  <Skeleton className="h-5 w-32" />
                  <Skeleton className="h-4 w-48 mt-1" />
                </div>
              </div>
              <Skeleton className="h-6 w-12" />
            </div>
          ))}
        </CardContent>
      </Card>
    );
  }

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (Object.keys(formValues).length > 0) {
      updateSettings(formValues);
    }
  };

  const handleSwitchChange = async (field: keyof UpdateSettingsRequest, checked: boolean) => {
    try {
      setFormValues(prev => ({ ...prev, [field]: checked }));
      await updateSettings({ [field]: checked });
    } catch (error) {
      setFormValues(prev => ({ ...prev, [field]: !checked }));
      console.error(`Failed to update ${field}:`, error);
    }
  };

  const handleChange = (field: keyof UpdateSettingsRequest, value: SettingsValue) => {
    setFormValues(prev => ({ ...prev, [field]: value }));
  };

  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-lg">Notifications</CardTitle>
        <CardDescription>Configure how you receive backup notifications</CardDescription>
      </CardHeader>
      <CardContent>
        <form onSubmit={handleSubmit} className="space-y-6">
          <div className="flex items-start gap-3 p-4 rounded-lg border bg-blue-50 dark:bg-blue-950/20 border-blue-200 dark:border-blue-900">
            <Info className="w-5 h-5 text-blue-600 dark:text-blue-400 mt-0.5 flex-shrink-0" />
            <div className="space-y-1">
              <p className="text-sm font-medium text-blue-900 dark:text-blue-100">
                Backup Notifications
              </p>
              <p className="text-xs text-blue-700 dark:text-blue-300">
                You will receive notifications for both successful and failed backups based on your configured notification channels.
              </p>
            </div>
          </div>

          {/* Dashboard Notifications */}
          <div className="flex items-center justify-between p-4 rounded-lg border bg-background/50">
            <div className="flex items-center gap-3">
              <div className="p-2 rounded-md bg-primary/10">
                <Bell className="w-4 h-4 text-primary" />
              </div>
              <div>
                <p className="text-sm font-medium">Dashboard Notifications</p>
                <p className="text-xs text-muted-foreground">
                  Show notifications in the dashboard
                </p>
              </div>
            </div>
            <Switch
              checked={settings?.notify_dashboard ?? false}
              disabled={isUpdating}
              onCheckedChange={(checked) => handleSwitchChange('notify_dashboard', checked)}
            />
          </div>

          {/* Email Notifications */}
          <div className="space-y-3">
            <div className="flex items-center justify-between p-4 rounded-lg border bg-background/50">
              <div className="flex items-center gap-3">
                <div className="p-2 rounded-md bg-primary/10">
                  <Mail className="w-4 h-4 text-primary" />
                </div>
                <div>
                  <p className="text-sm font-medium">Email Notifications</p>
                  <p className="text-xs text-muted-foreground">
                    Receive notifications via email
                  </p>
                </div>
              </div>
              <Switch
                checked={settings?.notify_email ?? false}
                disabled={isUpdating}
                onCheckedChange={(checked) => handleSwitchChange('notify_email', checked)}
              />
            </div>

            {settings?.notify_email && (
              <div className="ml-4 pl-6 border-l-2 space-y-4">
                <div className="grid gap-2">
                  <Label className="text-sm">
                    Email Address
                    {settings?.env_configured?.email && (
                      <span className="ml-2 text-xs text-muted-foreground">(set via environment variable)</span>
                    )}
                  </Label>
                  <Input
                    type="email"
                    placeholder="your@email.com"
                    defaultValue={settings?.email || ''}
                    onChange={(e) => handleChange('email', e.target.value)}
                    disabled={settings?.env_configured?.email}
                    className={settings?.env_configured?.email ? 'bg-muted cursor-not-allowed' : ''}
                  />
                </div>
                
                <div className="grid grid-cols-2 gap-4">
                  <div className="grid gap-2">
                    <Label className="text-sm">
                      SMTP Host
                      {settings?.env_configured?.smtp_host && (
                        <span className="ml-2 text-xs text-muted-foreground">(env var)</span>
                      )}
                    </Label>
                    <Input
                      placeholder="smtp.gmail.com"
                      defaultValue={settings.smtp_host}
                      onChange={(e) => handleChange('smtp_host', e.target.value)}
                      disabled={settings?.env_configured?.smtp_host}
                      className={settings?.env_configured?.smtp_host ? 'bg-muted cursor-not-allowed' : ''}
                    />
                  </div>
                  <div className="grid gap-2">
                    <Label className="text-sm">
                      SMTP Port
                      {settings?.env_configured?.smtp_port && (
                        <span className="ml-2 text-xs text-muted-foreground">(env var)</span>
                      )}
                    </Label>
                    <Input
                      type="number"
                      placeholder="587"
                      defaultValue={settings?.smtp_port?.toString() || ''}
                      onChange={(e) => handleChange('smtp_port', parseInt(e.target.value))}
                      disabled={settings?.env_configured?.smtp_port}
                      className={settings?.env_configured?.smtp_port ? 'bg-muted cursor-not-allowed' : ''}
                    />
                  </div>
                </div>

                <div className="grid gap-2">
                  <Label className="text-sm">
                    SMTP Username
                    {settings?.env_configured?.smtp_username && (
                      <span className="ml-2 text-xs text-muted-foreground">(env var)</span>
                    )}
                  </Label>
                  <Input
                    placeholder="username"
                    defaultValue={settings.smtp_username}
                    onChange={(e) => handleChange('smtp_username', e.target.value)}
                    disabled={settings?.env_configured?.smtp_username}
                    className={settings?.env_configured?.smtp_username ? 'bg-muted cursor-not-allowed' : ''}
                  />
                </div>
                
                <div className="grid gap-2">
                  <Label className="text-sm">
                    SMTP Password
                    {settings?.env_configured?.smtp_password && (
                      <span className="ml-2 text-xs text-muted-foreground">(env var)</span>
                    )}
                  </Label>
                  <Input
                    type="password"
                    placeholder="••••••••"
                    onChange={(e) => handleChange('smtp_password', e.target.value)}
                    disabled={settings?.env_configured?.smtp_password}
                    className={settings?.env_configured?.smtp_password ? 'bg-muted cursor-not-allowed' : ''}
                  />
                  <p className="text-xs text-muted-foreground">
                    Leave empty to keep current password
                  </p>
                </div>
              </div>
            )}
          </div>

          {/* Telegram Notifications */}
          <div className="space-y-3">
            <div className="flex items-center justify-between p-4 rounded-lg border bg-background/50">
              <div className="flex items-center gap-3">
                <div className="p-2 rounded-md bg-primary/10">
                  <Send className="w-4 h-4 text-primary" />
                </div>
                <div>
                  <p className="text-sm font-medium">Telegram Notifications</p>
                  <p className="text-xs text-muted-foreground">
                    Receive notifications via Telegram bot
                  </p>
                </div>
              </div>
              <Switch
                checked={settings?.notify_telegram ?? false}
                disabled={isUpdating}
                onCheckedChange={(checked) => handleSwitchChange('notify_telegram', checked)}
              />
            </div>

            {settings?.notify_telegram && (
              <div className="ml-4 pl-6 border-l-2 space-y-4">
                <div className="grid gap-2">
                  <Label className="text-sm">Telegram Bot Token</Label>
                  <div className="flex gap-2">
                    <Input
                      type="password"
                      placeholder="1234567890:ABCdefGHIjklMNOpqrsTUVwxyz"
                      value={telegramBotToken}
                      onChange={(e) => {
                        setTelegramBotToken(e.target.value);
                        handleChange('telegram_bot_token', e.target.value);
                      }}
                      className="font-mono text-xs"
                    />
                    <Button
                      type="button"
                      variant="outline"
                      size="sm"
                      onClick={async () => {
                        if (!telegramBotToken) {
                          toast({
                            title: "Error",
                            description: "Please enter a bot token first",
                            variant: "destructive",
                          });
                          return;
                        }
                        setIsLoadingChats(true);
                        try {
                          const response = await getTelegramChats(telegramBotToken);
                          setAvailableChats(response.data || []);
                          if (response.data && response.data.length > 0) {
                            toast({
                              title: "Success",
                              description: `Found ${response.data.length} chat(s)`,
                            });
                          } else {
                            toast({
                              title: "Info",
                              description: "No recent chats found. Make sure your bot has received messages.",
                            });
                          }
                        } catch (error: any) {
                          toast({
                            title: "Error",
                            description: error.message || "Failed to load chats",
                            variant: "destructive",
                          });
                        } finally {
                          setIsLoadingChats(false);
                        }
                      }}
                      disabled={isLoadingChats || !telegramBotToken}
                    >
                      {isLoadingChats ? (
                        <>
                          <Loader2 className="w-4 h-4 mr-2 animate-spin" />
                          Loading...
                        </>
                      ) : (
                        "Load Chats"
                      )}
                    </Button>
                  </div>
                  <p className="text-xs text-muted-foreground">
                    Get your bot token from <a href="https://t.me/BotFather" target="_blank" rel="noopener noreferrer" className="text-primary underline">@BotFather</a>
                  </p>
                </div>
                
                <div className="grid gap-2">
                  <Label className="text-sm">Telegram Chat ID</Label>
                  <div className="flex gap-2">
                    {availableChats.length > 0 ? (
                      <Select
                        value={telegramChatID}
                        onValueChange={(value) => {
                          setTelegramChatID(value);
                          handleChange('telegram_chat_id', value);
                        }}
                      >
                        <SelectTrigger>
                          <SelectValue placeholder="Select a chat or enter ID manually" />
                        </SelectTrigger>
                        <SelectContent>
                          {availableChats.map((chat) => {
                            const displayName = chat.title || 
                              `${chat.first_name || ''} ${chat.last_name || ''}`.trim() || 
                              chat.username || 
                              `Chat ${chat.id}`;
                            return (
                              <SelectItem key={chat.id} value={chat.id}>
                                <div className="flex flex-col">
                                  <span>{displayName}</span>
                                  <span className="text-xs text-muted-foreground">
                                    {chat.type} • ID: {chat.id}
                                  </span>
                                </div>
                              </SelectItem>
                            );
                          })}
                        </SelectContent>
                      </Select>
                    ) : (
                      <Input
                        placeholder="123456789"
                        value={telegramChatID}
                        onChange={(e) => {
                          setTelegramChatID(e.target.value);
                          handleChange('telegram_chat_id', e.target.value);
                        }}
                      />
                    )}
                    <Button
                      type="button"
                      variant="outline"
                      size="sm"
                      onClick={async () => {
                        if (!telegramBotToken || !telegramChatID) {
                          toast({
                            title: "Error",
                            description: "Please enter both bot token and chat ID",
                            variant: "destructive",
                          });
                          return;
                        }
                        setIsTestingConnection(true);
                        try {
                          const response = await testTelegramConnection({
                            telegram_bot_token: telegramBotToken,
                            telegram_chat_id: telegramChatID,
                          });
                          setChatInfo(response.data);
                          toast({
                            title: "Success",
                            description: `Connection successful! Chat: ${response.data.title || response.data.first_name || response.data.id}`,
                          });
                        } catch (error: any) {
                          setChatInfo(null);
                          toast({
                            title: "Error",
                            description: error.message || "Failed to test connection",
                            variant: "destructive",
                          });
                        } finally {
                          setIsTestingConnection(false);
                        }
                      }}
                      disabled={isTestingConnection || !telegramBotToken || !telegramChatID}
                    >
                      {isTestingConnection ? (
                        <>
                          <Loader2 className="w-4 h-4 mr-2 animate-spin" />
                          Testing...
                        </>
                      ) : (
                        "Test"
                      )}
                    </Button>
                  </div>
                  {chatInfo && (
                    <div className="flex items-center gap-2 p-2 rounded-md bg-green-50 dark:bg-green-950/20 border border-green-200 dark:border-green-900">
                      <CheckCircle2 className="w-4 h-4 text-green-600 dark:text-green-400" />
                      <div className="flex-1">
                        <p className="text-xs font-medium text-green-900 dark:text-green-100">
                          Connected to: {chatInfo.title || chatInfo.first_name || chatInfo.id}
                        </p>
                        <p className="text-xs text-green-700 dark:text-green-300">
                          Type: {chatInfo.type}
                        </p>
                      </div>
                    </div>
                  )}
                  <p className="text-xs text-muted-foreground">
                    {availableChats.length === 0 && "Your Telegram user ID or group chat ID. Send a message to "}
                    {availableChats.length === 0 && <a href="https://t.me/userinfobot" target="_blank" rel="noopener noreferrer" className="text-primary underline">@userinfobot</a>}
                    {availableChats.length === 0 && " to get your ID, or click 'Load Chats' to see available chats."}
                    {availableChats.length > 0 && "Select a chat from the list above, or enter a chat ID manually."}
                  </p>
                </div>
              </div>
            )}
          </div>

          {/* Webhook Notifications - Commented out until proper implementation */}
          {/* TODO: Implement proper webhook formatting for Slack/Discord/etc */}
          {/* <div className="space-y-3">
            <div className="flex items-center justify-between p-4 rounded-lg border bg-background/50">
              <div className="flex items-center gap-3">
                <div className="p-2 rounded-md bg-primary/10">
                  <Webhook className="w-4 h-4 text-primary" />
                </div>
                <div>
                  <p className="text-sm font-medium">Webhook Notifications</p>
                  <p className="text-xs text-muted-foreground">
                    Send notifications to a webhook URL
                  </p>
                </div>
              </div>
              <Switch
                checked={settings?.notify_webhook ?? false}
                disabled={isUpdating}
                onCheckedChange={(checked) => handleSwitchChange('notify_webhook', checked)}
              />
            </div>

            {settings?.notify_webhook && (
              <div className="ml-4 pl-6 border-l-2">
                <div className="grid gap-2">
                  <Label className="text-sm">Webhook URL</Label>
                  <Input
                    placeholder="https://hooks.slack.com/services/..."
                    defaultValue={settings.webhook_url}
                    onChange={(e) => handleChange('webhook_url', e.target.value)}
                  />
                  <p className="text-xs text-muted-foreground">
                    Supports Slack, Discord, and custom webhooks
                  </p>
                </div>
              </div>
            )}
          </div> */}

          {Object.keys(formValues).length > 0 && (
            <div className="pt-4 border-t flex justify-end">
              <Button type="submit" disabled={isUpdating}>
                {isUpdating ? "Saving..." : "Save Changes"}
              </Button>
            </div>
          )}
        </form>
      </CardContent>
    </Card>
  );
}
