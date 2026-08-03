"use client";

import React, { useState, useEffect } from 'react';
import { getUserMemories, deleteUserMemory, UserMemory } from '@/lib/memory_api';
import { FaTimes, FaBrain, FaTrash, FaShieldAlt, FaMagic } from 'react-icons/fa';

interface UserMemoriesModalProps {
  isOpen: boolean;
  onClose: () => void;
  userId?: string;
}

const CATEGORY_COLORS: Record<string, string> = {
  Profile: 'bg-blue-500/20 text-blue-300 border-blue-500/30',
  Preference: 'bg-purple-500/20 text-purple-300 border-purple-500/30',
  Skill: 'bg-emerald-500/20 text-emerald-300 border-emerald-500/30',
  Goal: 'bg-amber-500/20 text-amber-300 border-amber-500/30',
  Project: 'bg-cyan-500/20 text-cyan-300 border-cyan-500/30',
  Interest: 'bg-pink-500/20 text-pink-300 border-pink-500/30',
};

export const UserMemoriesModal: React.FC<UserMemoriesModalProps> = ({ isOpen, onClose, userId = 'default_user' }) => {
  const [memories, setMemories] = useState<UserMemory[]>([]);
  const [loading, setLoading] = useState(false);

  const loadMemories = () => {
    setLoading(true);
    getUserMemories(userId)
      .then(setMemories)
      .catch((err) => console.error('Failed to load user memories:', err))
      .finally(() => setLoading(false));
  };

  useEffect(() => {
    if (isOpen) {
      loadMemories();
    }
  }, [isOpen, userId]);

  if (!isOpen) return null;

  const handleDelete = async (memoryId: string) => {
    try {
      await deleteUserMemory(memoryId);
      setMemories((prev) => prev.filter((m) => m.memory_id !== memoryId));
    } catch (err) {
      console.error('Failed to delete memory:', err);
    }
  };

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/60 backdrop-blur-sm p-4">
      <div className="w-full max-w-2xl rounded-2xl bg-slate-900 border border-slate-800 p-6 shadow-2xl text-slate-100 animate-in fade-in zoom-in duration-200">
        <div className="flex items-center justify-between border-b border-slate-800 pb-4 mb-4">
          <div className="flex items-center gap-2 text-purple-400">
            <FaBrain className="w-6 h-6" />
            <div>
              <h2 className="text-xl font-bold text-white">Long-Term User Memories</h2>
              <p className="text-xs text-slate-400">Automatically extracted durable facts and preferences about you</p>
            </div>
          </div>
          <button onClick={onClose} className="p-1 rounded-lg text-slate-400 hover:text-white hover:bg-slate-800 transition">
            <FaTimes className="w-5 h-5" />
          </button>
        </div>

        {loading ? (
          <div className="py-12 text-center text-slate-400 flex flex-col items-center gap-2">
            <FaMagic className="w-6 h-6 animate-spin text-purple-400" />
            <p className="text-sm">Loading memories...</p>
          </div>
        ) : memories.length === 0 ? (
          <div className="py-12 text-center text-slate-500">
            <p className="text-sm">No memories extracted yet.</p>
            <p className="text-xs text-slate-600 mt-1">As you chat, the assistant will automatically remember your skills, projects, and goals!</p>
          </div>
        ) : (
          <div className="space-y-3 max-h-[60vh] overflow-y-auto pr-1">
            {memories.map((mem) => (
              <div
                key={mem.memory_id}
                className="flex items-center justify-between p-3.5 rounded-xl bg-slate-800/60 border border-slate-700/50 hover:border-slate-600 transition"
              >
                <div className="flex items-start gap-3">
                  <span className={`px-2.5 py-1 rounded-full text-xs font-semibold border ${CATEGORY_COLORS[mem.memory_type] || 'bg-slate-700 text-slate-300'}`}>
                    {mem.memory_type}
                  </span>
                  <div>
                    <p className="text-sm text-slate-200 font-medium">{mem.memory_text}</p>
                    <div className="flex items-center gap-2 mt-1 text-[11px] text-slate-400">
                      <span className="flex items-center gap-1">
                        <FaShieldAlt className="w-3.5 h-3.5 text-emerald-400" /> Confidence: {(mem.confidence_score * 100).toFixed(0)}%
                      </span>
                      <span>•</span>
                      <span>Extracted: {new Date(mem.created_at).toLocaleDateString()}</span>
                    </div>
                  </div>
                </div>
                <button
                  onClick={() => handleDelete(mem.memory_id)}
                  title="Forget this memory"
                  className="p-1.5 rounded-lg text-slate-400 hover:text-red-400 hover:bg-slate-700 transition"
                >
                  <FaTrash className="w-4 h-4" />
                </button>
              </div>
            ))}
          </div>
        )}

        <div className="border-t border-slate-800 pt-4 mt-6 flex justify-end">
          <button
            onClick={onClose}
            className="px-4 py-2 bg-slate-800 hover:bg-slate-700 text-slate-200 rounded-lg text-sm font-medium transition"
          >
            Close
          </button>
        </div>
      </div>
    </div>
  );
};
