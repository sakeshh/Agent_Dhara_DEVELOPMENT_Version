'use client';

import { useRouter } from 'next/navigation';
import { useEffect } from 'react';

export default function ChatPage() {
  const router = useRouter();

  useEffect(() => {
    router.replace('/data-pipeline?chat=open');
  }, [router]);

  return (
    <div className="min-h-screen flex items-center justify-center bg-zinc-50 dark:bg-zinc-950 text-zinc-600 dark:text-zinc-400">
      <div className="flex items-center gap-3">
        <div className="w-5 h-5 border-2 border-[#0070AD] border-t-transparent rounded-full animate-spin" />
        <span className="text-sm font-medium">Opening Agent Dhara Unified Workspace...</span>
      </div>
    </div>
  );
}
