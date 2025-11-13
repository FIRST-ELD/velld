"use client";

import { useState } from "react";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { LogOut } from "lucide-react";
import { logout } from "@/lib/helper";

export function SignOutCard() {
  const [isLoading, setIsLoading] = useState(false);

  const handleLogout = () => {
    setIsLoading(true);
    logout();
  };

  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-lg">Sign Out</CardTitle>
        <CardDescription>End your current session</CardDescription>
      </CardHeader>
      <CardContent>
        <Button 
          variant="destructive" 
          onClick={handleLogout}
          disabled={isLoading}
          className="w-full sm:w-auto"
        >
          <LogOut className="w-4 h-4 mr-2" />
          {isLoading ? "Signing out..." : "Sign out"}
        </Button>
      </CardContent>
    </Card>
  );
}

