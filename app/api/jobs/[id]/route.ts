import { NextRequest, NextResponse } from 'next/server';
import { proxyToBackend } from '@/lib/backend-bridge';

export const dynamic = 'force-dynamic';

export async function GET(
  request: NextRequest,
  { params }: { params: { id: string } }
) {
  try {
    const res = await proxyToBackend(`/jobs/${params.id}`, {
      method: 'GET',
      // Chat jobs previously returned the full graph `session` blob; keep headroom for large legacy rows.
      timeoutMs: 120_000,
    });
    const data = await res.json();
    return NextResponse.json(data, { status: res.status });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
