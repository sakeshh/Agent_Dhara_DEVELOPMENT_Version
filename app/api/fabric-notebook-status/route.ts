import { NextRequest, NextResponse } from 'next/server';
import { proxyToBackend } from '@/lib/backend-bridge';

export const dynamic = 'force-dynamic';

export async function GET(req: NextRequest) {
  const { searchParams } = new URL(req.url);
  const notebookId = searchParams.get('notebook_id');
  const runId = searchParams.get('run_id');
  
  if (!notebookId || !runId) {
    return NextResponse.json({ error: 'Missing notebook_id or run_id' }, { status: 400 });
  }
  
  const res = await proxyToBackend(`/fabric/run-status/${notebookId}/${runId}`, {
    method: 'GET',
  });
  const text = await res.text();
  return new NextResponse(text, {
    status: res.status,
    headers: { 'Content-Type': res.headers.get('content-type') ?? 'application/json' },
  });
}
