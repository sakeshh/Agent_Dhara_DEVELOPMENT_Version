'use client';

import React, { useState, useEffect, useRef, useCallback } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import {
  FaRobot,
  FaTimes,
  FaExpandAlt,
  FaCompressAlt,
  FaColumns,
  FaGripVertical,
  FaChevronRight,
  FaCommentDots,
} from 'react-icons/fa';
import ChatWindow from './ChatWindow';

interface IdeChatDrawerProps {
  isOpen: boolean;
  onToggle: () => void;
  gxEnabled?: boolean;
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

type SizePreset = 'compact' | 'standard' | 'wide' | 'half';

const PRESET_WIDTHS: Record<SizePreset, number> = {
  compact: 380,
  standard: 480,
  wide: 640,
  half: 800,
};

export default function IdeChatDrawer({
  isOpen,
  onToggle,
  gxEnabled = false,
  pipelineState,
  pipelineActions,
}: IdeChatDrawerProps) {

  const [width, setWidth] = useState<number>(480);
  const [activePreset, setActivePreset] = useState<SizePreset>('standard');
  const [isResizing, setIsResizing] = useState<boolean>(false);
  const isDraggingRef = useRef<boolean>(false);
  const startXRef = useRef<number>(0);
  const startWidthRef = useRef<number>(480);

  // Keyboard shortcut Ctrl + K or Cmd + K to toggle assistant
  useEffect(() => {
    const handleKeyDown = (e: KeyboardEvent) => {
      if ((e.ctrlKey || e.metaKey) && e.key.toLowerCase() === 'k') {
        e.preventDefault();
        onToggle();
      }
    };
    window.addEventListener('keydown', handleKeyDown);
    return () => window.removeEventListener('keydown', handleKeyDown);
  }, [onToggle]);

  // Handle Drag Resizing
  const handleMouseDown = useCallback((e: React.MouseEvent) => {
    e.preventDefault();
    isDraggingRef.current = true;
    startXRef.current = e.clientX;
    startWidthRef.current = width;
    setIsResizing(true);
    document.body.style.cursor = 'ew-resize';
    document.body.style.userSelect = 'none';
  }, [width]);

  useEffect(() => {
    const handleMouseMove = (e: MouseEvent) => {
      if (!isDraggingRef.current) return;
      const deltaX = startXRef.current - e.clientX;
      const newWidth = Math.max(340, Math.min(window.innerWidth * 0.75, startWidthRef.current + deltaX));
      setWidth(newWidth);
    };

    const handleMouseUp = () => {
      if (isDraggingRef.current) {
        isDraggingRef.current = false;
        setIsResizing(false);
        document.body.style.cursor = '';
        document.body.style.userSelect = '';
      }
    };

    window.addEventListener('mousemove', handleMouseMove);
    window.addEventListener('mouseup', handleMouseUp);
    return () => {
      window.removeEventListener('mousemove', handleMouseMove);
      window.removeEventListener('mouseup', handleMouseUp);
    };
  }, []);

  const setPresetWidth = (preset: SizePreset) => {
    setActivePreset(preset);
    if (preset === 'half') {
      setWidth(Math.round(window.innerWidth * 0.5));
    } else {
      setWidth(PRESET_WIDTHS[preset]);
    }
  };

  return (
    <>
      {/* Floating Action Button when collapsed */}
      <AnimatePresence>
        {!isOpen && (
          <motion.button
            initial={{ scale: 0, opacity: 0 }}
            animate={{ scale: 1, opacity: 1 }}
            exit={{ scale: 0, opacity: 0 }}
            onClick={onToggle}
            whileHover={{ scale: 1.08 }}
            whileTap={{ scale: 0.94 }}
            className="fixed bottom-6 right-6 z-50 flex items-center gap-3 px-5 py-3.5 rounded-full bg-[#0070AD] text-white font-semibold shadow-2xl hover:bg-[#005c8f] transition-all ring-4 ring-[#0070AD]/20 group"
          >
            <div className="relative">
              <FaRobot className="text-xl group-hover:rotate-12 transition-transform" />
              <span className="absolute -top-1 -right-1 w-2.5 h-2.5 bg-emerald-400 rounded-full animate-ping" />
              <span className="absolute -top-1 -right-1 w-2.5 h-2.5 bg-emerald-400 rounded-full" />
            </div>
            <span className="text-sm">Agent Dhara Assistant</span>
            <span className="text-[10px] bg-white/20 px-2 py-0.5 rounded-full font-mono">Ctrl+K</span>
          </motion.button>
        )}
      </AnimatePresence>

      {/* Slide-over Resizable Assistant Panel */}
      <AnimatePresence>
        {isOpen && (
          <motion.div
            initial={{ x: '100%', opacity: 0 }}
            animate={{ x: 0, opacity: 1 }}
            exit={{ x: '100%', opacity: 0 }}
            transition={{ type: 'spring', damping: 25, stiffness: 220 }}
            style={{ width: `${width}px` }}
            className="fixed top-0 right-0 bottom-0 z-50 bg-white/95 dark:bg-slate-950/95 backdrop-blur-2xl border-l border-black/10 dark:border-slate-800 shadow-2xl flex flex-col overflow-hidden"
          >
            {/* Left Drag Resize Handle */}
            <div
              onMouseDown={handleMouseDown}
              className={`absolute top-0 left-0 bottom-0 w-2.5 -translate-x-1/2 cursor-ew-resize hover:bg-[#0070AD]/40 transition-colors z-50 flex items-center justify-center group ${
                isResizing ? 'bg-[#0070AD]' : ''
              }`}
            >
              <div className="w-1 h-8 rounded-full bg-black/20 dark:bg-white/20 group-hover:bg-[#0070AD] transition-colors" />
            </div>

            {/* IDE Drawer Header Bar */}
            <div className="px-4 py-3 bg-zinc-900 text-white flex items-center justify-between border-b border-zinc-800 shrink-0">
              <div className="flex items-center gap-2.5">
                <div className="p-1.5 rounded-lg bg-[#0070AD] text-white">
                  <FaRobot className="text-sm" />
                </div>
                <div>
                  <h3 className="text-xs font-bold uppercase tracking-wider text-white">Agent Dhara AI Assistant</h3>
                  <p className="text-[10px] text-zinc-400">IDE Conversational Pair Programmer</p>
                </div>
              </div>

              {/* Preset Width Controls & Collapse */}
              <div className="flex items-center gap-1.5">
                <div className="hidden sm:flex items-center gap-1 bg-zinc-800/80 p-1 rounded-lg border border-zinc-700">
                  <button
                    onClick={() => setPresetWidth('compact')}
                    title="Compact width (380px)"
                    className={`px-2 py-0.5 text-[10px] rounded font-medium transition ${
                      activePreset === 'compact' ? 'bg-[#0070AD] text-white' : 'text-zinc-400 hover:text-white'
                    }`}
                  >
                    380px
                  </button>
                  <button
                    onClick={() => setPresetWidth('standard')}
                    title="Standard width (480px)"
                    className={`px-2 py-0.5 text-[10px] rounded font-medium transition ${
                      activePreset === 'standard' ? 'bg-[#0070AD] text-white' : 'text-zinc-400 hover:text-white'
                    }`}
                  >
                    480px
                  </button>
                  <button
                    onClick={() => setPresetWidth('wide')}
                    title="Wide width (640px)"
                    className={`px-2 py-0.5 text-[10px] rounded font-medium transition ${
                      activePreset === 'wide' ? 'bg-[#0070AD] text-white' : 'text-zinc-400 hover:text-white'
                    }`}
                  >
                    640px
                  </button>
                  <button
                    onClick={() => setPresetWidth('half')}
                    title="Half Screen (50%)"
                    className={`px-2 py-0.5 text-[10px] rounded font-medium transition ${
                      activePreset === 'half' ? 'bg-[#0070AD] text-white' : 'text-zinc-400 hover:text-white'
                    }`}
                  >
                    50%
                  </button>
                </div>

                <button
                  onClick={onToggle}
                  title="Close AI Assistant (Ctrl+K)"
                  className="p-1.5 rounded-lg text-zinc-400 hover:text-white hover:bg-zinc-800 transition"
                >
                  <FaTimes className="text-sm" />
                </button>
              </div>
            </div>

              <ChatWindow
                gxEnabled={gxEnabled}
                onClose={onToggle}
                pipelineState={pipelineState}
                pipelineActions={pipelineActions}
              />

          </motion.div>
        )}
      </AnimatePresence>
    </>
  );
}
