'use client';

import { useState, useEffect } from 'react';
import { motion } from 'framer-motion';
import { FaFileAlt, FaCheckCircle, FaSearch, FaTable, FaDatabase } from 'react-icons/fa';

interface FileSelectorProps {
  database: string;
  onSelect: (files: string[], availableFiles: string[]) => void;
  onNext: () => void;
  selectedFiles: string[];
}

type Item = { name: string; size: string; rows: number; type: string };

function parseSourceToken(token: string): { kind: 'sql' | 'blob' | 'streams' | 'unknown'; index: number } {
  const parts = String(token || '').split(':');
  const kind = (parts[1] as any) || 'unknown';
  const index = Number(parts[2] || 0);
  return { kind, index: Number.isFinite(index) ? index : 0 };
}

export default function FileSelector({ database, onSelect, onNext, selectedFiles }: FileSelectorProps) {
  const [files, setFiles] = useState<Item[]>([]);
  const [searchTerm, setSearchTerm] = useState('');
  const [loading, setLoading] = useState(true);
  const [selected, setSelected] = useState<string[]>(selectedFiles);

  const loadSqlTables = async (opts?: { bypassCache?: boolean }) => {
    const bypassCache = Boolean(opts?.bypassCache);
    setLoading(true);
    try {
      const ttl = bypassCache ? 0 : 30;
      const res = await fetch(`/api/schema/tables?ttl_seconds=${ttl}`, { method: 'GET' });
      const data = await res.json().catch(() => null);
      const tables = Array.isArray(data?.tables) ? data.tables : [];
      setFiles(
        tables.map((t: string) => ({
          name: String(t),
          size: 'SQL table',
          rows: 0,
          type: 'table',
        }))
      );
    } catch {
      try {
        const res = await fetch('/api/list-tables', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: '',
        });
        const data = await res.json().catch(() => null);
        const tables = Array.isArray(data?.tables) ? data.tables : [];
        setFiles(
          tables.map((t: string) => ({
            name: String(t),
            size: 'SQL table',
            rows: 0,
            type: 'table',
          }))
        );
      } catch {
        setFiles([]);
      }
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    let alive = true;
    (async () => {
      setLoading(true);
      const parsed = parseSourceToken(database);
      if (parsed.kind === 'sql') {
        await loadSqlTables({ bypassCache: false });
        return;
      }

      const ALL_CONTAINER_BLOBS = [
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
      ];

      if (parsed.kind === 'blob') {
        try {
          const res = await fetch('/api/blob-files');
          const data = await res.json().catch(() => null);
          const rawFiles = Array.isArray(data?.files) && data.files.length > 0 ? data.files : ALL_CONTAINER_BLOBS;
          if (!alive) return;
          setFiles(
            rawFiles.map((name: string) => ({
              name: String(name),
              size: 'Blob file',
              rows: 0,
              type: String(name).split('.').pop() || 'file',
            }))
          );
        } catch {
          if (!alive) return;
          setFiles(
            ALL_CONTAINER_BLOBS.map((name: string) => ({
              name: String(name),
              size: 'Blob file',
              rows: 0,
              type: String(name).split('.').pop() || 'file',
            }))
          );
        } finally {
          if (alive) setLoading(false);
        }
        return;
      }

      try {
        const res = await fetch('/api/sources');
        const data = await res.json().catch(() => null);
        const locs = Array.isArray(data?.locations) ? data.locations : [];
        const loc = locs.find((x: any) => Number(x?.index) === parsed.index) || locs[0];
        const rawFiles = Array.isArray(loc?.files) && loc.files.length > 0 ? loc.files : ALL_CONTAINER_BLOBS;
        if (!alive) return;
        setFiles(
          rawFiles.map((name: string) => ({
            name: String(name),
            size: 'File stream',
            rows: 0,
            type: String(name).split('.').pop() || 'file',
          }))
        );
      } catch {
        if (!alive) return;
        setFiles(
          ALL_CONTAINER_BLOBS.map((name: string) => ({
            name: String(name),
            size: 'File stream',
            rows: 0,
            type: String(name).split('.').pop() || 'file',
          }))
        );
      } finally {
        if (alive) setLoading(false);
      }
    })();

    return () => {
      alive = false;
    };
  }, [database]);

  const parsedSource = parseSourceToken(database);
  const isSqlSource = parsedSource.kind === 'sql';

  const toggleFile = (fileName: string) => {
    let updated: string[];
    if (selected.includes(fileName)) {
      updated = selected.filter((f) => f !== fileName);
    } else {
      updated = [...selected, fileName];
    }
    setSelected(updated);
    onSelect(
      updated,
      files.map((f) => f.name)
    );
  };

  const toggleSelectAll = () => {
    if (selected.length === files.length) {
      setSelected([]);
      onSelect([], files.map((f) => f.name));
    } else {
      const allNames = files.map((f) => f.name);
      setSelected(allNames);
      onSelect(allNames, allNames);
    }
  };

  const filteredFiles = files.filter((file) =>
    file.name.toLowerCase().includes(searchTerm.toLowerCase())
  );

  return (
    <div className="space-y-6">
      <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-4">
        <div>
          <h2 className="text-3xl font-bold text-zinc-900 mb-2">
            {isSqlSource ? 'Select Tables / Objects' : 'Select Files'}
          </h2>
          <p className="text-black/60">
            {isSqlSource
              ? 'Choose target database tables to run assessment & ETL generation on'
              : 'Choose one or more data files to process'}
          </p>
        </div>
        <div className="flex items-center gap-3">
          {isSqlSource && (
            <motion.button
              type="button"
              onClick={() => loadSqlTables({ bypassCache: true })}
              whileHover={{ scale: 1.02 }}
              whileTap={{ scale: 0.98 }}
              className="px-3.5 py-2.5 rounded-xl border border-black/10 bg-white/80 text-sm font-medium text-black/70 hover:bg-white hover:border-[#0070AD]/30 transition-colors shadow-sm"
            >
              Refresh DB catalog
            </motion.button>
          )}
          {files.length > 0 && (
            <motion.button
              type="button"
              onClick={toggleSelectAll}
              className="px-4 py-2 text-sm font-semibold text-[#0070AD] hover:bg-[#0070AD]/5 rounded-xl transition-colors"
              whileHover={{ scale: 1.02 }}
              whileTap={{ scale: 0.98 }}
            >
              {selected.length === files.length ? 'Deselect all' : 'Select all'}
            </motion.button>
          )}
          {selected.length > 0 && (
            <motion.button
              type="button"
              onClick={onNext}
              className="px-6 py-2.5 bg-[#0070AD] text-white font-semibold rounded-xl hover:bg-[#005c8f] shadow-lg shadow-[#0070AD]/20 transition-colors flex items-center gap-2"
              whileHover={{ scale: 1.02 }}
              whileTap={{ scale: 0.98 }}
            >
              Continue with {selected.length} {isSqlSource ? 'table' : 'file'}{selected.length > 1 ? 's' : ''} →
            </motion.button>
          )}
        </div>
      </div>

      <div className="relative">
        <FaSearch className="absolute left-4 top-1/2 -translate-y-1/2 text-black/45 text-lg" />
        <input
          type="text"
          placeholder={isSqlSource ? 'Search tables by name...' : 'Search files by name...'}
          value={searchTerm}
          onChange={(e) => setSearchTerm(e.target.value)}
          className="w-full pl-12 pr-4 py-3.5 border border-black/15 rounded-xl focus:ring-2 focus:ring-[#0070AD]/25 focus:border-[#0070AD]/50 outline-none bg-white text-zinc-900 placeholder-black/40 text-sm font-medium shadow-sm"
        />
      </div>

      {loading ? (
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
          {[1, 2, 3, 4, 5, 6].map((i) => (
            <div key={i} className="h-32 bg-white/40 animate-pulse rounded-2xl border border-black/5" />
          ))}
        </div>
      ) : (
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
          {filteredFiles.map((file, index) => {
            const isSelected = selected.includes(file.name);
            return (
              <motion.div
                key={file.name}
                onClick={() => toggleFile(file.name)}
                className={`p-5 rounded-2xl border-2 cursor-pointer transition-all ${
                  isSelected
                    ? 'bg-[#0070AD]/10 border-[#0070AD] shadow-lg text-zinc-900 ring-2 ring-[#0070AD]/20'
                    : 'bg-white border-black/10 text-black/80 hover:border-[#0070AD]/40 hover:shadow-md'
                }`}
                initial={{ opacity: 0, y: 20 }}
                animate={{ opacity: 1, y: 0 }}
                transition={{ delay: index * 0.04 }}
                whileHover={{ scale: 1.02 }}
                whileTap={{ scale: 0.98 }}
              >
                <div className="flex items-start justify-between gap-3">
                  <div className="flex items-center gap-3 min-w-0">
                    <div className={`p-3 rounded-xl ${isSelected ? 'bg-[#0070AD] text-white' : 'bg-[#0070AD]/10 text-[#0070AD]'}`}>
                      {isSqlSource ? <FaTable className="text-xl" /> : <FaFileAlt className="text-xl" />}
                    </div>
                    <div className="min-w-0">
                      <h3 className="font-bold text-zinc-900 text-sm truncate">{file.name}</h3>
                      <p className="text-xs text-black/55 capitalize mt-0.5">{file.type}</p>
                    </div>
                  </div>
                  {isSelected && <FaCheckCircle className="text-[#0070AD] text-xl shrink-0" />}
                </div>
              </motion.div>
            );
          })}
        </div>
      )}

      {filteredFiles.length === 0 && !loading && (
        <div className="text-center py-12">
          {isSqlSource ? (
            <FaDatabase className="text-6xl text-black/30 mx-auto mb-4" />
          ) : (
            <FaFileAlt className="text-6xl text-black/30 mx-auto mb-4" />
          )}
          <p className="text-black/60 text-base font-medium">
            {isSqlSource ? 'No database tables found' : 'No data files found in this source'}
          </p>
        </div>
      )}
    </div>
  );
}
