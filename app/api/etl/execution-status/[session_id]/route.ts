import { NextRequest, NextResponse } from 'next/server';
import { getBackendBaseUrl, proxyToBackend } from '@/lib/backend-bridge';

export const dynamic = 'force-dynamic';

export async function GET(
  _: NextRequest,
  { params }: { params: { session_id: string } }
) {
  if (!getBackendBaseUrl()) {
    return NextResponse.json(
      { ok: false, error: 'BACKEND_NOT_CONFIGURED', message: 'Set BACKEND_BASE_URL' },
      { status: 503 }
    );
  }
  try {
    const res = await proxyToBackend(`/etl/execution-status/${encodeURIComponent(params.session_id)}`, {
      method: 'GET',
      timeoutMs: 30_000,
    });
    const text = await res.text();
    return new NextResponse(text, {
      status: res.status,
      headers: { 'Content-Type': res.headers.get('content-type') ?? 'application/json' },
    });
  } catch (err: unknown) {
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ ok: false, error: 'PROXY_FAILED', message }, { status: 500 });
  }
}
