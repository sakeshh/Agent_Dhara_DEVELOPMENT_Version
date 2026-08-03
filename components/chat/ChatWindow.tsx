'use client';

import { useState, useRef, useEffect, useCallback } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import {
  FaDatabase,
  FaCloud,
  FaStream,
  FaPlug,
  FaFolder,
  FaChevronDown,
  FaArrowLeft,
  FaCopy,
  FaEdit,
  FaStop,
  FaPlus,
  FaFileUpload,
  FaExclamationTriangle,
  FaCheck,
  FaHistory,
  FaEllipsisV,
  FaTrash,
  FaFilePdf,
  FaFileCode,
  FaFileAlt,
  FaImage,
  FaArrowUp,
  FaTimes,
  FaClock,
  FaBroom,
  FaSync,
  FaFileExport,
  FaSearch,
  FaRobot,
} from 'react-icons/fa';
import { Message, AttachedFile } from '@/types';
import Image from 'next/image';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';
import ReportEnhancements from '@/components/ReportEnhancements';
import EtlGenerationPanel from '@/components/EtlGenerationPanel';
import {
  humanizeSeverityLabel,
  humanizeSnakeIdentifier,
  prettyTableHeaderKey,
  sourceRootLabel,
} from '@/lib/assessmentDisplay';

interface ChatOption {
  id: string;
  text: string;
  category: string;
  action?: string;
  icon?: React.ReactNode;
}

interface StoredConversation {
  id: string;
  title: string;
  updatedAt: string;
  messages: Message[];
}

const DATA_SOURCE_OPTIONS: ChatOption[] = [
  { id: 'local', text: 'Local data', category: 'Data Source', action: 'local-data', icon: <FaFolder className="text-[#0070AD]/80" /> },
  { id: 'sql', text: 'SQL data', category: 'Data Source', action: 'sql-data', icon: <FaDatabase className="text-[#0070AD]/80" /> },
  { id: 'blob', text: 'Blob data', category: 'Data Source', action: 'blob-data', icon: <FaCloud className="text-[#12ABDB]/80" /> },
  { id: 'streams', text: 'Real-time streams', category: 'Data Source', action: 'realtime-streams', icon: <FaStream className="text-[#0070AD]/80" /> },
  { id: 'apis', text: 'APIs', category: 'Data Source', action: 'apis', icon: <FaPlug className="text-[#12ABDB]/80" /> },
];

const CHAT_OPTIONS_BY_SOURCE: Record<string, ChatOption[]> = {
  sql: [
    { id: 'sql-view', text: '👁️ View data in files/tables', category: 'Explore', action: 'guided-view' },
    { id: 'sql-report', text: '📄 Extract report from files/tables', category: 'Reports', action: 'guided-report' },
  ],
  local: [
    { id: 'local-view', text: '👁️ View data in files', category: 'Explore', action: 'guided-view' },
    { id: 'local-report', text: '📄 Extract report from files', category: 'Reports', action: 'guided-report' },
  ],
  blob: [
    { id: 'blob-view', text: '👁️ View data in files', category: 'Explore', action: 'guided-view' },
    { id: 'blob-report', text: '📄 Extract report from files', category: 'Reports', action: 'guided-report' },
  ],
  streams: [
    { id: 'streams-view', text: '👁️ View data in stream files', category: 'Explore', action: 'guided-view' },
    { id: 'streams-report', text: '📄 Extract report from stream files', category: 'Reports', action: 'guided-report' },
  ],
  apis: [
    { id: 'apis-1', text: '📊 Start Data Pipeline Workflow', category: 'Pipeline', action: '/data-pipeline' },
    { id: 'apis-2', text: '🔗 Connect to REST / GraphQL API', category: 'API', action: 'connect' },
    { id: 'apis-2b', text: '📄 View Report', category: 'Reports', action: 'view-report' },
    { id: 'apis-3', text: '📥 Fetch API Data', category: 'Import', action: 'fetch' },
    { id: 'apis-4', text: '🔄 Transform Data', category: 'Transform', action: 'transform' },
    { id: 'apis-5', text: '📤 Export Data', category: 'Export', action: 'export' },
    { id: 'apis-6', text: '🔍 Analyze API Response', category: 'Analysis', action: 'analyze' },
  ],
};

function getChatOptionsForSource(sourceId: string | null): ChatOption[] {
  if (!sourceId) return [];
  return CHAT_OPTIONS_BY_SOURCE[sourceId] ?? CHAT_OPTIONS_BY_SOURCE.local;
}

/** Robust browser PDF text extractor */
async function extractPdfText(file: File): Promise<string> {
  try {
    const arrayBuffer = await file.arrayBuffer();
    const bytes = new Uint8Array(arrayBuffer);
    const textDecoder = new TextDecoder('latin1');
    const pdfString = textDecoder.decode(bytes);

    const textBlocks: string[] = [];
    const regex = /BT[\s\S]*?ET/g;
    let match;
    while ((match = regex.exec(pdfString)) !== null) {
      const block = match[0];
      const stringMatches = block.match(/\((.*?)\)/g);
      if (stringMatches) {
        const line = stringMatches
          .map((s) => s.slice(1, -1).replace(/\\([()\\])/g, '$1'))
          .join(' ');
        if (line.trim()) textBlocks.push(line.trim());
      }
    }

    if (textBlocks.length > 0) {
      return textBlocks.join('\n');
    }

    const printable = pdfString
      .replace(/[^\x20-\x7E\n\r\t]/g, ' ')
      .split(/\n+/)
      .map((line) => line.trim())
      .filter((line) => line.length > 3 && !line.startsWith('%') && !line.includes('obj') && !line.includes('endobj'));

    return printable.join('\n').slice(0, 10000) || `Extracted text from PDF document (${file.name})`;
  } catch (err) {
    return `PDF File Content (${file.name}, ${(file.size / 1024).toFixed(1)} KB)`;
  }
}

/** Process file attachments (Image, PDF, Text/Code) */
async function processAttachedFiles(files: File[]): Promise<AttachedFile[]> {
  const result: AttachedFile[] = [];
  for (const file of files) {
    const id = `att-${Date.now()}-${Math.random().toString(36).substr(2, 5)}`;
    const isImg = file.type.startsWith('image/') || /\.(png|jpe?g|webp|gif|svg)$/i.test(file.name);
    const isPdf = file.type === 'application/pdf' || file.name.toLowerCase().endsWith('.pdf');

    if (isImg) {
      const dataUrl = await new Promise<string>((resolve) => {
        const reader = new FileReader();
        reader.onload = () => resolve(reader.result as string);
        reader.readAsDataURL(file);
      });
      result.push({
        id,
        name: file.name,
        type: 'image',
        size: file.size,
        dataUrl,
        extractedText: `[Image Attachment: ${file.name} (${(file.size / 1024).toFixed(1)} KB)]`,
      });
    } else if (isPdf) {
      const text = await extractPdfText(file);
      result.push({
        id,
        name: file.name,
        type: 'pdf',
        size: file.size,
        extractedText: text,
      });
    } else {
      const text = await new Promise<string>((resolve) => {
        const reader = new FileReader();
        reader.onload = () => resolve(reader.result as string);
        reader.readAsText(file);
      }).catch(() => `[File: ${file.name}]`);
      result.push({
        id,
        name: file.name,
        type: 'file',
        size: file.size,
        extractedText: text,
      });
    }
  }
  return result;
}

/** Format time timestamp string */
function formatTime(d: Date | string): string {
  const dateObj = typeof d === 'string' ? new Date(d) : d;
  if (isNaN(dateObj.getTime())) return '';
  const h = dateObj.getHours();
  const m = dateObj.getMinutes();
  const ampm = h >= 12 ? 'PM' : 'AM';
  const h12 = h % 12 || 12;
  const min = m < 10 ? `0${m}` : String(m);
  return `${h12}:${min} ${ampm}`;
}

/** Format generation latency time */
function formatLatency(ms?: number): string | null {
  if (!ms || ms <= 0) return null;
  if (ms < 1000) return `${ms}ms`;
  return `${(ms / 1000).toFixed(2)}s`;
}

interface ChatWindowProps {
  gxEnabled?: boolean;
  onClose?: () => void;
  pipelineState?: {
    currentStep?: string;
    selectedDatabase?: string | null;
    selectedFiles?: string[];
    assessmentData?: any;
    dqGate?: any;
    etlCode?: string | null;
    execResult?: any;
  };
  pipelineActions?: {
    onSelectDatabase?: (db: string) => void;
    onSelectFiles?: (files: string[], available: string[]) => void;
    onRunAssessment?: () => void;
    onGenerateEtl?: () => void;
    onExecuteRemediation?: (approved?: boolean, dryRun?: boolean) => void;
    onGoToStep?: (stepId: string) => void;
  };
}

export default function ChatWindow({ gxEnabled = false, onClose, pipelineState, pipelineActions }: ChatWindowProps) {
  const [messages, setMessages] = useState<Message[]>([]);
  const [hasSelectedDataSource, setHasSelectedDataSource] = useState(false);
  const [selectedDataSource, setSelectedDataSource] = useState<string | null>(null);
  const [agentError, setAgentError] = useState<string | null>(null);
  const [showOptions, setShowOptions] = useState(false);
  const [chatInput, setChatInput] = useState('');
  const [isLoadingAgent, setIsLoadingAgent] = useState(false);
  const [jobProgress, setJobProgress] = useState<string | null>(null);
  const [agentThreadId, setAgentThreadId] = useState<string | null>(null);
  const [sessionId, setSessionId] = useState<string>('default');

  // File Attachments state
  const [attachedFiles, setAttachedFiles] = useState<AttachedFile[]>([]);
  const [isProcessingFiles, setIsProcessingFiles] = useState(false);

  // Past Conversations & Navigation state
  const [pastConversations, setPastConversations] = useState<StoredConversation[]>([]);
  const [isHistoryOpen, setIsHistoryOpen] = useState(false);
  const [isMoreOptionsOpen, setIsMoreOptionsOpen] = useState(false);
  const [historySearchQuery, setHistorySearchQuery] = useState('');
  const [editingConvId, setEditingConvId] = useState<string | null>(null);
  const [editingConvTitle, setEditingConvTitle] = useState('');

  // Message CRUD state
  const [editingMessageId, setEditingMessageId] = useState<string | null>(null);
  const [editingMessageText, setEditingMessageText] = useState('');
  const [copiedMessageId, setCopiedMessageId] = useState<string | null>(null);

  const [localFolderFiles, setLocalFolderFiles] = useState<File[]>([]);
  const [guidedMode, setGuidedMode] = useState<'none' | 'view' | 'report'>('none');
  const [pendingTableSelections, setPendingTableSelections] = useState<Record<string, number[]>>({});
  const [pendingFileSelections, setPendingFileSelections] = useState<
    Record<string, { mode: 'blob' | 'local'; selected: number[] }>
  >({});

  const messagesEndRef = useRef<HTMLDivElement>(null);
  const messagesScrollRef = useRef<HTMLDivElement>(null);
  const abortRef = useRef<AbortController | null>(null);
  const fileInputRef = useRef<HTMLInputElement | null>(null);
  const multiFileInputRef = useRef<HTMLInputElement | null>(null);
  const localFolderInputRef = useRef<HTMLInputElement | null>(null);

  const scrollToBottom = () => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  };

  useEffect(() => {
    scrollToBottom();
  }, [messages, isLoadingAgent, jobProgress, attachedFiles]);

  // Load conversations from local storage
  const loadConversationsFromStorage = useCallback(() => {
    if (typeof window === 'undefined') return;
    try {
      const raw = window.localStorage.getItem('agentDhara_conversations');
      if (raw) {
        const parsed: StoredConversation[] = JSON.parse(raw);
        setPastConversations(parsed);
      }
    } catch {
      // ignore
    }
  }, []);

  // Save conversations to local storage
  const saveConversationsToStorage = (convs: StoredConversation[]) => {
    if (typeof window === 'undefined') return;
    try {
      window.localStorage.setItem('agentDhara_conversations', JSON.stringify(convs));
      setPastConversations(convs);
    } catch {
      // ignore
    }
  };

  useEffect(() => {
    loadConversationsFromStorage();
  }, [loadConversationsFromStorage]);

  // Sync current conversation to past conversations storage
  useEffect(() => {
    if (!sessionId || messages.length === 0) return;
    if (typeof window === 'undefined') return;

    try {
      const raw = window.localStorage.getItem('agentDhara_conversations');
      const existing: StoredConversation[] = raw ? JSON.parse(raw) : [];
      const userMsgs = messages.filter((m) => m.sender === 'user');
      const title = userMsgs.length > 0 ? userMsgs[0].text.slice(0, 32) + '...' : 'Conversation ' + sessionId.slice(0, 6);

      const idx = existing.findIndex((c) => c.id === sessionId);
      const updatedItem: StoredConversation = {
        id: sessionId,
        title: idx >= 0 ? existing[idx].title || title : title,
        updatedAt: new Date().toISOString(),
        messages,
      };

      let newConvs: StoredConversation[];
      if (idx >= 0) {
        newConvs = [...existing];
        newConvs[idx] = updatedItem;
      } else {
        newConvs = [updatedItem, ...existing];
      }
      window.localStorage.setItem('agentDhara_conversations', JSON.stringify(newConvs));
      setPastConversations(newConvs);
    } catch {
      // ignore
    }
  }, [messages, sessionId]);

  // Start new conversation
  const startNewConversation = () => {
    const newId = 'conv-' + Date.now();
    setSessionId(newId);
    if (typeof window !== 'undefined') {
      window.localStorage.setItem('dharaSessionId', newId);
    }
    setMessages([
      {
        id: `greeting-${Date.now()}`,
        text: "Hi, I'm Agent Dhara! How can I assist you today?",
        sender: 'bot',
        timestamp: new Date(),
        options: [
          { id: 'sql', text: '1. SQL Data', send: 'sql' },
          { id: 'blob', text: '2. Blob Data', send: 'blob' },
          { id: 'fs', text: '3. Local Files', send: 'local' },
        ],
      },
    ]);
    setAttachedFiles([]);
    setAgentError(null);
    setShowOptions(false);
    setIsHistoryOpen(false);
    setIsMoreOptionsOpen(false);
  };

  // Switch to a past conversation
  const loadConversation = (conv: StoredConversation) => {
    setSessionId(conv.id);
    if (typeof window !== 'undefined') {
      window.localStorage.setItem('dharaSessionId', conv.id);
    }
    setMessages(
      conv.messages.map((m) => ({
        ...m,
        timestamp: new Date(m.timestamp),
      }))
    );
    setAttachedFiles([]);
    setIsHistoryOpen(false);
  };

  // Delete a conversation from history
  const deleteStoredConversation = (id: string, e: React.MouseEvent) => {
    e.stopPropagation();
    const updated = pastConversations.filter((c) => c.id !== id);
    saveConversationsToStorage(updated);
    if (sessionId === id) {
      startNewConversation();
    }
  };

  // Save renamed conversation title
  const saveConvRename = (id: string, e: React.FormEvent) => {
    e.preventDefault();
    if (!editingConvTitle.trim()) return;
    const updated = pastConversations.map((c) =>
      c.id === id ? { ...c, title: editingConvTitle.trim() } : c
    );
    saveConversationsToStorage(updated);
    setEditingConvId(null);
  };

  // Clean / Clear chat
  const handleCleanChat = () => {
    setMessages([
      {
        id: `greeting-${Date.now()}`,
        text: "Chat history cleaned. Ready for your next query!",
        sender: 'bot',
        timestamp: new Date(),
      },
    ]);
    setAttachedFiles([]);
    setIsMoreOptionsOpen(false);
  };

  const getPersistedSelectedSource = (): string | null => {
    if (typeof window === 'undefined') return null;
    const v = window.localStorage.getItem('dharaSelectedDataSource');
    return v && typeof v === 'string' ? v : null;
  };

  const getEffectiveSessionId = (): string => {
    if (typeof window === 'undefined') return 'default';
    return window.localStorage.getItem('dharaSessionId') || 'default';
  };

  useEffect(() => {
    if (typeof window === 'undefined') return;
    const saved = window.localStorage.getItem('agentThreadId');
    if (saved && !agentThreadId) setAgentThreadId(saved);
    setSessionId(getEffectiveSessionId());
    const persistedSource = getPersistedSelectedSource();
    if (persistedSource && !selectedDataSource) {
      setSelectedDataSource(persistedSource);
      setHasSelectedDataSource(true);
    }
  }, [agentThreadId, selectedDataSource]);

  // Initial greeting
  useEffect(() => {
    if (messages.length === 0) {
      setMessages([
        {
          id: `greeting-${Date.now()}`,
          text: "Hi, I'm Agent Dhara! How can I assist you today?",
          sender: 'bot',
          timestamp: new Date(),
          options: [
            { id: 'sql', text: '1. SQL Data', send: 'sql' },
            { id: 'blob', text: '2. Blob Data', send: 'blob' },
            { id: 'fs', text: '3. Local Files', send: 'local' },
          ],
        },
      ]);
    }
  }, [messages.length]);

  /** Build request payload for Azure agent from current messages */
  const getMessagesForApi = (msgs: Message[]) =>
    msgs.map((m) => ({
      role: m.sender === 'user' ? ('user' as const) : ('assistant' as const),
      content: m.text,
    }));

  /** Poll for job status until completion */
  const pollJobStatus = async (jobId: string): Promise<any> => {
    const startTime = Date.now();
    const TIMEOUT = 1200000;
    const INTERVAL = 2000;

    while (Date.now() - startTime < TIMEOUT) {
      if (abortRef.current?.signal.aborted) throw new Error('ABORTED');

      try {
        const eventsRes = await fetch(`/api/jobs/${jobId}/events`);
        if (eventsRes.ok) {
          const { events } = await eventsRes.json();
          if (Array.isArray(events) && events.length > 0) {
            const last = events[events.length - 1];
            setJobProgress(last.message || 'Processing...');
          }
        }
      } catch {
        // ignore
      }

      let res;
      let retryCount = 0;
      const MAX_RETRIES = 3;

      while (retryCount < MAX_RETRIES) {
        try {
          res = await fetch(`/api/jobs/${jobId}`);
          if (res.ok || (res.status !== 502 && res.status !== 504 && res.status !== 503)) {
            break;
          }
        } catch {
          // ignore
        }
        retryCount++;
        await new Promise((resolve) => setTimeout(resolve, 1000));
      }

      if (!res || !res.ok) {
        throw new Error(`Polling failed (${res?.status || 'network_error'})`);
      }

      const rawJobData = await res.json();
      const job = rawJobData.job || rawJobData;
      if (job.status === 'succeeded') {
        setJobProgress(null);
        return rawJobData.result || job.result;
      }
      if (job.status === 'failed') {
        setJobProgress(null);
        throw new Error(job.error || 'Job failed');
      }

      await new Promise((resolve) => setTimeout(resolve, INTERVAL));
    }
    throw new Error('Job timed out');
  };

  /** Call agent reply API */
  const fetchAgentReply = async (
    msgs: Message[]
  ): Promise<{ content: string; threadId: string | null; payload: any }> => {
    abortRef.current?.abort();
    const controller = new AbortController();
    abortRef.current = controller;

    const res = await fetch('/api/jobs', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      signal: controller.signal,
      body: JSON.stringify({
        kind: 'chat',
        input: {
          messages: getMessagesForApi(msgs),
          threadId: agentThreadId,
          session_id: sessionId,
          message: msgs[msgs.length - 1].text,
          gx_enabled: gxEnabled,
        },
      }),
    });

    const data = await res.json();
    if (!res.ok || !data.job_id) {
      throw new Error(data?.error || 'Failed to create job');
    }

    const result = await pollJobStatus(data.job_id);
    const content = typeof result?.reply === 'string' ? result.reply : '';
    const threadId = result?.threadId ?? null;
    const payload = result?.payload ?? null;

    if (!content.trim() && !payload) {
      throw new Error('EMPTY_REPLY');
    }
    return { content, threadId, payload };
  };

  const stopGeneration = () => {
    abortRef.current?.abort();
    abortRef.current = null;
    setIsLoadingAgent(false);
  };

  // Copy text helper with indicator
  const copyText = async (text: string, msgId?: string) => {
    try {
      await navigator.clipboard.writeText(text);
      if (msgId) {
        setCopiedMessageId(msgId);
        setTimeout(() => setCopiedMessageId(null), 2000);
      }
    } catch {
      // ignore
    }
  };

  // Delete message
  const deleteMessage = (id: string) => {
    setMessages((prev) => prev.filter((m) => m.id !== id));
  };

  // Edit user message
  const startEditMessage = (msg: Message) => {
    setEditingMessageId(msg.id);
    setEditingMessageText(msg.text);
  };

  const saveEditMessage = async (msgId: string) => {
    if (!editingMessageText.trim()) return;
    const newText = editingMessageText.trim();
    setEditingMessageId(null);

    // Replace message text and re-trigger response
    const updatedMessages = messages.map((m) => (m.id === msgId ? { ...m, text: newText } : m));
    setMessages(updatedMessages);
    await sendUserText(newText, [], true, updatedMessages);
  };

  // Handle multi-file attachment selection
  const handleFileSelect = async (e: React.ChangeEvent<HTMLInputElement>) => {
    const files = Array.from(e.target.files || []);
    e.target.value = '';
    if (files.length === 0) return;

    setIsProcessingFiles(true);
    try {
      const processed = await processAttachedFiles(files);
      setAttachedFiles((prev) => [...prev, ...processed]);
    } catch (err) {
      console.error('Failed to process files:', err);
    } finally {
      setIsProcessingFiles(false);
    }
  };

  // Remove attached file pill
  const removeAttachedFile = (id: string) => {
    setAttachedFiles((prev) => prev.filter((f) => f.id !== id));
  };

  /** Send user message with attachments & track generation duration */
  const sendUserText = async (
    userText: string,
    currentAttachments: AttachedFile[] = attachedFiles,
    isReSend = false,
    customMessagesList?: Message[]
  ) => {
    setAgentError(null);
    setJobProgress(null);

    // Build context prompt including extracted information from files/PDFs/images
    let promptContext = userText;
    if (currentAttachments.length > 0) {
      const infoBlocks = currentAttachments
        .map((att) => {
          if (att.type === 'pdf') {
            return `--- [ATTACHED PDF DOCUMENT: ${att.name}] ---\n${att.extractedText || ''}\n----------------------------------`;
          }
          if (att.type === 'image') {
            return `--- [ATTACHED IMAGE: ${att.name}] ---\nImage preview attached. Details: ${att.size} bytes.\n----------------------------------`;
          }
          return `--- [ATTACHED FILE: ${att.name}] ---\n${att.extractedText || ''}\n----------------------------------`;
        })
        .join('\n\n');
      promptContext = `${infoBlocks}\n\nUser Question:\n${userText}`;
    }

    const userMsg: Message = {
      id: String(Date.now()),
      text: userText,
      sender: 'user',
      timestamp: new Date(),
      attachments: currentAttachments.length > 0 ? [...currentAttachments] : undefined,
    };

    const baseList = customMessagesList || messages;
    const nextMsgs = isReSend ? baseList : [...baseList, userMsg];
    if (!isReSend) {
      setMessages(nextMsgs);
    }
    setAttachedFiles([]);

    setIsLoadingAgent(true);
    const startTimeMs = performance.now();

    try {
      // Modify latest message text for API call to include full extracted attachment context
      const apiMessages = nextMsgs.map((m, idx) =>
        idx === nextMsgs.length - 1 ? { ...m, text: promptContext } : m
      );

      const reply = await fetchAgentReply(apiMessages);
      const endTimeMs = performance.now();
      const latencyMs = Math.round(endTimeMs - startTimeMs);

      if (reply.threadId) {
        setAgentThreadId(reply.threadId);
      }

      setMessages((prev) => [
        ...prev,
        {
          id: String(Date.now() + 1),
          text: reply.content,
          sender: 'bot',
          timestamp: new Date(),
          generationTimeMs: latencyMs,
          payload: reply.payload,
        },
      ]);
    } catch (err: any) {
      const errorMsg = err?.message === 'ABORTED' ? 'Generation stopped.' : err?.message || String(err);
      setAgentError(errorMsg);
      setMessages((prev) => [
        ...prev,
        {
          id: String(Date.now() + 1),
          text: `⚠️ Agent Dhara encountered an issue: ${errorMsg}`,
          sender: 'bot',
          timestamp: new Date(),
        },
      ]);
    } finally {
      setIsLoadingAgent(false);
      setJobProgress(null);
    }
  };

  const handleOptionSelect = async (option: ChatOption) => {
    setSelectedDataSource(option.id);
    setHasSelectedDataSource(true);
    await sendUserText(option.text);
  };

  const handleBack = () => {
    setHasSelectedDataSource(false);
    setSelectedDataSource(null);
  };

  const filteredHistory = pastConversations.filter((c) =>
    c.title.toLowerCase().includes(historySearchQuery.toLowerCase())
  );

  return (
    <div className="flex flex-col h-full bg-[#0d0d0f] text-zinc-100 relative overflow-hidden font-sans select-none">
      {/* Top Header Bar matching User Image */}
      <div className="px-4 py-3 bg-[#16161a] border-b border-white/10 flex items-center justify-between shrink-0 z-30 shadow-md">
        <div className="flex items-center gap-3">
          <div className="flex items-center gap-2">
            <span className="w-2.5 h-2.5 rounded-full bg-emerald-400 shadow-[0_0_8px_rgba(52,211,153,0.8)] animate-pulse" />
            <h2 className="text-base font-extrabold text-white tracking-tight font-sans">
              Agent Dhara
            </h2>
          </div>
        </div>

        {/* Header Control Icons: + (New), Clock (History), ... (Options), X (Close) */}
        <div className="flex items-center gap-1">
          <button
            onClick={startNewConversation}
            title="Start new conversation"
            className="p-2 rounded-lg text-zinc-300 hover:text-white hover:bg-white/10 transition-colors"
          >
            <FaPlus className="text-sm" />
          </button>

          <button
            onClick={() => setIsHistoryOpen(!isHistoryOpen)}
            title="Check past conversations"
            className={`p-2 rounded-lg transition-colors ${
              isHistoryOpen ? 'text-[#12ABDB] bg-[#12ABDB]/10' : 'text-zinc-300 hover:text-white hover:bg-white/10'
            }`}
          >
            <FaClock className="text-sm" />
          </button>

          <div className="relative">
            <button
              onClick={() => setIsMoreOptionsOpen(!isMoreOptionsOpen)}
              title="More options & clean chat"
              className={`p-2 rounded-lg transition-colors ${
                isMoreOptionsOpen ? 'text-white bg-white/10' : 'text-zinc-300 hover:text-white hover:bg-white/10'
              }`}
            >
              <FaEllipsisV className="text-sm" />
            </button>

            {/* More Options Dropdown Menu */}
            <AnimatePresence>
              {isMoreOptionsOpen && (
                <>
                  <div
                    className="fixed inset-0 z-40"
                    onClick={() => setIsMoreOptionsOpen(false)}
                  />
                  <motion.div
                    initial={{ opacity: 0, scale: 0.95, y: -5 }}
                    animate={{ opacity: 1, scale: 1, y: 0 }}
                    exit={{ opacity: 0, scale: 0.95, y: -5 }}
                    className="absolute right-0 top-10 z-50 w-52 rounded-xl bg-[#1e1e24] border border-white/10 shadow-2xl p-1.5 space-y-1"
                  >
                    <button
                      onClick={handleCleanChat}
                      className="w-full flex items-center gap-2.5 px-3 py-2 text-xs font-medium text-rose-300 hover:bg-rose-500/10 rounded-lg transition-colors"
                    >
                      <FaBroom className="text-rose-400" />
                      <span>Clean Chat</span>
                    </button>
                    <button
                      onClick={startNewConversation}
                      className="w-full flex items-center gap-2.5 px-3 py-2 text-xs font-medium text-zinc-200 hover:bg-white/5 rounded-lg transition-colors"
                    >
                      <FaPlus className="text-indigo-400" />
                      <span>New Conversation</span>
                    </button>
                    <button
                      onClick={() => {
                        copyText(messages.map((m) => `${m.sender.toUpperCase()}: ${m.text}`).join('\n\n'));
                        setIsMoreOptionsOpen(false);
                      }}
                      className="w-full flex items-center gap-2.5 px-3 py-2 text-xs font-medium text-zinc-200 hover:bg-white/5 rounded-lg transition-colors"
                    >
                      <FaFileExport className="text-emerald-400" />
                      <span>Export Conversation</span>
                    </button>
                  </motion.div>
                </>
              )}
            </AnimatePresence>
          </div>

          {onClose && (
            <button
              onClick={onClose}
              title="Close panel"
              className="p-2 rounded-lg text-zinc-400 hover:text-white hover:bg-white/10 transition-colors ml-1"
            >
              <FaTimes className="text-sm" />
            </button>
          )}
        </div>
      </div>

      {/* Past Conversations History Drawer (Clock icon modal) */}
      <AnimatePresence>
        {isHistoryOpen && (
          <>
            <motion.div
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              exit={{ opacity: 0 }}
              onClick={() => setIsHistoryOpen(false)}
              className="absolute inset-0 bg-black/60 backdrop-blur-sm z-40"
            />
            <motion.div
              initial={{ x: '-100%' }}
              animate={{ x: 0 }}
              exit={{ x: '-100%' }}
              transition={{ type: 'spring', damping: 25, stiffness: 220 }}
              className="absolute top-0 left-0 bottom-0 w-80 bg-[#16161a] border-r border-white/10 shadow-2xl z-50 flex flex-col"
            >
              <div className="p-4 border-b border-white/10 flex items-center justify-between">
                <div className="flex items-center gap-2 text-white font-bold text-sm">
                  <FaHistory className="text-[#12ABDB]" />
                  <span>Past Conversations</span>
                </div>
                <button
                  onClick={() => setIsHistoryOpen(false)}
                  className="p-1 rounded text-zinc-400 hover:text-white"
                >
                  <FaTimes />
                </button>
              </div>

              {/* History Search & New Chat Button */}
              <div className="p-3 border-b border-white/5 space-y-2">
                <button
                  onClick={startNewConversation}
                  className="w-full flex items-center justify-center gap-2 py-2 px-3 bg-[#0070AD] hover:bg-[#005c8f] text-white rounded-lg text-xs font-semibold shadow transition"
                >
                  <FaPlus /> Start New Chat
                </button>

                <div className="relative">
                  <input
                    type="text"
                    placeholder="Search past chats..."
                    value={historySearchQuery}
                    onChange={(e) => setHistorySearchQuery(e.target.value)}
                    className="w-full bg-[#202026] border border-white/10 rounded-lg py-1.5 pl-8 pr-3 text-xs text-zinc-200 placeholder-zinc-500 focus:outline-none focus:border-[#0070AD]"
                  />
                  <FaSearch className="w-3 h-3 text-zinc-500 absolute left-2.5 top-2.5" />
                </div>
              </div>

              {/* Conversations List */}
              <div className="flex-1 overflow-y-auto p-2 space-y-1">
                {filteredHistory.length === 0 ? (
                  <div className="text-center py-8 text-xs text-zinc-500">
                    No past conversations found.
                  </div>
                ) : (
                  filteredHistory.map((conv) => {
                    const isActive = conv.id === sessionId;
                    return (
                      <div
                        key={conv.id}
                        onClick={() => loadConversation(conv)}
                        className={`group relative flex items-center justify-between p-2.5 rounded-lg cursor-pointer transition-all ${
                          isActive
                            ? 'bg-[#0070AD]/20 border border-[#0070AD]/40 text-white'
                            : 'hover:bg-white/5 text-zinc-300'
                        }`}
                      >
                        <div className="flex-1 min-w-0 pr-2">
                          {editingConvId === conv.id ? (
                            <form onSubmit={(e) => saveConvRename(conv.id, e)} onClick={(e) => e.stopPropagation()}>
                              <input
                                type="text"
                                autoFocus
                                value={editingConvTitle}
                                onChange={(e) => setEditingConvTitle(e.target.value)}
                                onBlur={(e) => saveConvRename(conv.id, e)}
                                className="w-full bg-[#101014] border border-white/20 rounded px-2 py-0.5 text-xs text-white outline-none"
                              />
                            </form>
                          ) : (
                            <>
                              <h4 className="text-xs font-medium truncate">{conv.title}</h4>
                              <p className="text-[10px] text-zinc-500 mt-0.5 flex items-center gap-2">
                                <span>{formatTime(conv.updatedAt)}</span>
                                <span>• {conv.messages?.length || 0} msgs</span>
                              </p>
                            </>
                          )}
                        </div>

                        {/* Conv Actions */}
                        <div className="flex items-center gap-1 opacity-0 group-hover:opacity-100 transition-opacity">
                          <button
                            onClick={(e) => {
                              e.stopPropagation();
                              setEditingConvId(conv.id);
                              setEditingConvTitle(conv.title);
                            }}
                            title="Rename"
                            className="p-1 text-zinc-400 hover:text-white"
                          >
                            <FaEdit className="text-xs" />
                          </button>
                          <button
                            onClick={(e) => deleteStoredConversation(conv.id, e)}
                            title="Delete"
                            className="p-1 text-zinc-400 hover:text-rose-400"
                          >
                            <FaTrash className="text-xs" />
                          </button>
                        </div>
                      </div>
                    );
                  })
                )}
              </div>
            </motion.div>
          </>
        )}
      </AnimatePresence>

      {/* Main Conversation Messages Body */}
      <div ref={messagesScrollRef} className="flex-1 overflow-y-auto p-4 space-y-6 scroll-smooth z-10">

        {messages.map((m) => {
          const isUser = m.sender === 'user';
          return (
            <motion.div
              key={m.id}
              initial={{ opacity: 0, y: 8 }}
              animate={{ opacity: 1, y: 0 }}
              className={`flex flex-col group ${isUser ? 'items-end' : 'items-start'}`}
            >
              <div
                className={`relative max-w-[88%] rounded-2xl p-4 shadow-lg leading-relaxed text-sm ${
                  isUser
                    ? 'bg-[#0070AD] text-white rounded-br-none'
                    : 'bg-[#1a1a20] border border-white/10 text-zinc-100 rounded-bl-none'
                }`}
              >
                {/* Attached files preview inside message */}
                {m.attachments && m.attachments.length > 0 && (
                  <div className="mb-3 space-y-2 border-b border-white/10 pb-2">
                    {m.attachments.map((att) => (
                      <div
                        key={att.id}
                        className="flex items-center gap-2 p-2 rounded-lg bg-black/30 border border-white/10 text-xs"
                      >
                        {att.type === 'image' && att.dataUrl ? (
                          <img src={att.dataUrl} alt={att.name} className="w-10 h-10 object-cover rounded" />
                        ) : att.type === 'pdf' ? (
                          <FaFilePdf className="text-rose-400 text-base" />
                        ) : (
                          <FaFileCode className="text-emerald-400 text-base" />
                        )}
                        <div className="flex-1 truncate">
                          <p className="font-semibold truncate">{att.name}</p>
                          <p className="text-[10px] opacity-70">
                            {(att.size / 1024).toFixed(1)} KB • {att.type.toUpperCase()}
                          </p>
                        </div>
                      </div>
                    ))}
                  </div>
                )}

                {/* Edit inline for User message */}
                {editingMessageId === m.id ? (
                  <div className="space-y-2">
                    <textarea
                      value={editingMessageText}
                      onChange={(e) => setEditingMessageText(e.target.value)}
                      className="w-full bg-black/40 border border-white/20 rounded-lg p-2 text-sm text-white outline-none"
                      rows={3}
                    />
                    <div className="flex items-center justify-end gap-2">
                      <button
                        onClick={() => setEditingMessageId(null)}
                        className="px-2.5 py-1 text-xs bg-white/10 rounded hover:bg-white/20"
                      >
                        Cancel
                      </button>
                      <button
                        onClick={() => saveEditMessage(m.id)}
                        className="px-2.5 py-1 text-xs bg-emerald-600 rounded text-white font-semibold"
                      >
                        Save & Resend
                      </button>
                    </div>
                  </div>
                ) : (
                  <div className="prose prose-invert max-w-none text-sm leading-relaxed break-words">
                    <ReactMarkdown remarkPlugins={[remarkGfm]}>{m.text}</ReactMarkdown>
                  </div>
                )}

                {/* Message Hover Action Menu (Copy, Edit, Delete) */}
                <div className="absolute top-2 right-2 opacity-0 group-hover:opacity-100 transition-opacity flex items-center gap-1.5 bg-black/60 backdrop-blur-md px-2 py-1 rounded-full border border-white/10">
                  <button
                    onClick={() => copyText(m.text, m.id)}
                    title="Copy text"
                    className="text-zinc-300 hover:text-white"
                  >
                    {copiedMessageId === m.id ? <FaCheck className="text-emerald-400 text-xs" /> : <FaCopy className="text-xs" />}
                  </button>
                  {isUser && (
                    <button onClick={() => startEditMessage(m)} title="Edit prompt" className="text-zinc-300 hover:text-white">
                      <FaEdit className="text-xs" />
                    </button>
                  )}
                  <button onClick={() => deleteMessage(m.id)} title="Delete message" className="text-zinc-300 hover:text-rose-400">
                    <FaTrash className="text-xs" />
                  </button>
                </div>

                {/* Timestamp and Generation Latency Footer on EVERY message */}
                <div className="mt-2.5 flex items-center justify-end gap-2 text-[10px] opacity-60 border-t border-white/5 pt-1 font-mono">
                  <span>{formatTime(m.timestamp)}</span>
                  {!isUser && m.generationTimeMs && (
                    <span className="text-emerald-400 font-semibold">
                      • ⚡ {formatLatency(m.generationTimeMs)}
                    </span>
                  )}
                </div>
              </div>
            </motion.div>
          );
        })}

        {/* Loading Agent state */}
        {isLoadingAgent && (
          <div className="flex items-center gap-3 p-3 rounded-xl bg-[#1a1a20] border border-white/10 w-fit text-xs text-zinc-400">
            <div className="w-4 h-4 border-2 border-[#12ABDB] border-t-transparent rounded-full animate-spin" />
            <span>Agent Dhara is thinking... {jobProgress ? `(${jobProgress})` : ''}</span>
            <button onClick={stopGeneration} className="ml-2 text-rose-400 hover:underline text-[10px]">
              Stop
            </button>
          </div>
        )}

        <div ref={messagesEndRef} />
      </div>

      {/* Hidden file inputs for attachment upload */}
      <input
        ref={multiFileInputRef}
        type="file"
        multiple
        accept="image/*,.pdf,.txt,.csv,.json,.md,.py,.sql,.js,.ts"
        className="hidden"
        onChange={handleFileSelect}
      />

      {/* FLOATING DARK INPUT CARD matching User Screenshot */}
      <div className="p-4 z-20 bg-gradient-to-t from-[#0d0d0f] via-[#0d0d0f] to-transparent">
        <div className="max-w-3xl mx-auto bg-[#1a1a20] border border-white/10 rounded-2xl p-3 shadow-2xl backdrop-blur-xl relative">
          {/* Previews of attached files/images/PDFs inside input card */}
          {attachedFiles.length > 0 && (
            <div className="flex flex-wrap gap-2 mb-2 pb-2 border-b border-white/10">
              {attachedFiles.map((att) => (
                <div
                  key={att.id}
                  className="flex items-center gap-2 bg-[#25252c] border border-white/10 rounded-lg px-2.5 py-1 text-xs text-zinc-200"
                >
                  {att.type === 'image' && att.dataUrl ? (
                    <img src={att.dataUrl} alt={att.name} className="w-6 h-6 object-cover rounded" />
                  ) : att.type === 'pdf' ? (
                    <FaFilePdf className="text-rose-400" />
                  ) : (
                    <FaFileAlt className="text-emerald-400" />
                  )}
                  <span className="max-w-[120px] truncate">{att.name}</span>
                  <button
                    onClick={() => removeAttachedFile(att.id)}
                    className="text-zinc-400 hover:text-rose-400 ml-1"
                  >
                    <FaTimes className="text-xs" />
                  </button>
                </div>
              ))}
            </div>
          )}

          {/* Text Area Input */}
          <textarea
            value={chatInput}
            onChange={(e) => setChatInput(e.target.value)}
            onKeyDown={async (e) => {
              if (e.key === 'Enter' && !e.shiftKey) {
                e.preventDefault();
                if ((chatInput.trim() || attachedFiles.length > 0) && !isLoadingAgent) {
                  const text = chatInput.trim();
                  setChatInput('');
                  await sendUserText(text);
                }
              }
            }}
            rows={2}
            placeholder="Ask anything, @ to mention, / for actions"
            className="w-full bg-transparent text-sm text-zinc-100 placeholder-zinc-500 outline-none resize-none px-1"
          />

          {/* Bottom Control Row inside Card */}
          <div className="flex items-center justify-between pt-2 border-t border-white/5 mt-1">
            {/* Left Controls: Upload + Agent Dhara Badge */}
            <div className="flex items-center gap-2">
              <button
                onClick={() => multiFileInputRef.current?.click()}
                title="Add image, files, or PDF"
                className="p-2 rounded-lg text-zinc-400 hover:text-white hover:bg-white/10 transition-colors"
              >
                <FaPlus className="text-sm" />
              </button>

              {/* Agent Dhara Badge replacing model selector per user requirement */}
              <div className="flex items-center gap-2 px-3 py-1 rounded-full bg-white/5 border border-white/10 text-xs font-semibold text-zinc-200">
                <span className="w-2 h-2 rounded-full bg-emerald-400 animate-pulse" />
                <span>Agent Dhara</span>
              </div>
            </div>

            {/* Right Control: Send Arrow Mark Button per user requirement */}
            <button
              disabled={(!chatInput.trim() && attachedFiles.length === 0) || isLoadingAgent}
              onClick={async () => {
                if ((chatInput.trim() || attachedFiles.length > 0) && !isLoadingAgent) {
                  const text = chatInput.trim();
                  setChatInput('');
                  await sendUserText(text);
                }
              }}
              className="p-2.5 rounded-full bg-[#0070AD] hover:bg-[#12ABDB] text-white disabled:opacity-30 disabled:cursor-not-allowed transition-all shadow-md active:scale-95"
              title="Send message"
            >
              <FaArrowUp className="text-sm" />
            </button>
          </div>
        </div>

        {/* Footer Warning */}
        <p className="text-[11px] text-center text-zinc-500 mt-2">
          AI may make mistakes. Double-check all generated code.
        </p>
      </div>
    </div>
  );
}
