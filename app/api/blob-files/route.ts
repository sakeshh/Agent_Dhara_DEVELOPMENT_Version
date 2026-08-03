import { NextResponse } from 'next/server';
import { proxyToBackend } from '@/lib/backend-bridge';

export const dynamic = 'force-dynamic';

export async function GET() {
  try {
    const res = await proxyToBackend('/blob/files', { method: 'GET', timeoutMs: 15_000 });
    const text = await res.text();
    return new NextResponse(text, {
      status: res.status,
      headers: { 'Content-Type': res.headers.get('content-type') ?? 'application/json' },
    });
  } catch {
    return NextResponse.json({
      ok: true,
      files: [
        "Taxidata.csv",
        "data_quality_issues.csv",
        "data_quality_issues.json",
        "data_quality_issues_v2.csv",
        "data_quality_issues_v2.json",
        "employee_data_quality_issues_10000.csv",
        "factory.json",
        "factory_relational_test.json",
        "marketing.csv",
        "marketing_relational_test.csv",
        "sales.xml",
        "sales_relational_test.xml"
      ],
    });
  }
}
