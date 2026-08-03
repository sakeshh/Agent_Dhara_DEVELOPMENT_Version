"use client";

import React, { useState, useEffect } from 'react';
import { getUserPreferences, updateUserPreferences, UserPreferences } from '@/lib/memory_api';
import { FaTimes, FaSave, FaSlidersH, FaCheck } from 'react-icons/fa';

interface UserPreferencesModalProps {
  isOpen: boolean;
  onClose: () => void;
  userId?: string;
}

export const UserPreferencesModal: React.FC<UserPreferencesModalProps> = ({ isOpen, onClose, userId = 'default_user' }) => {
  const [preferences, setPreferences] = useState<Partial<UserPreferences>>({
    preferred_language: 'English',
    preferred_coding_language: 'Python',
    preferred_response_length: 'detailed',
    preferred_tone: 'professional',
    preferred_depth: 'deep_dive',
    custom_instructions: '',
  });
  const [saving, setSaving] = useState(false);
  const [savedSuccess, setSavedSuccess] = useState(false);

  useEffect(() => {
    if (isOpen) {
      getUserPreferences(userId)
        .then(setPreferences)
        .catch((err) => console.error('Failed to load preferences:', err));
    }
  }, [isOpen, userId]);

  if (!isOpen) return null;

  const handleSave = async () => {
    setSaving(true);
    try {
      await updateUserPreferences(preferences, userId);
      setSavedSuccess(true);
      setTimeout(() => setSavedSuccess(false), 2000);
    } catch (err) {
      console.error('Failed to save preferences:', err);
    } finally {
      setSaving(false);
    }
  };

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/60 backdrop-blur-sm p-4">
      <div className="w-full max-w-lg rounded-2xl bg-slate-900 border border-slate-800 p-6 shadow-2xl text-slate-100 animate-in fade-in zoom-in duration-200">
        <div className="flex items-center justify-between border-b border-slate-800 pb-4 mb-4">
          <div className="flex items-center gap-2 text-indigo-400">
            <FaSlidersH className="w-5 h-5" />
            <h2 className="text-xl font-bold text-white">AI Personalization & Preferences</h2>
          </div>
          <button onClick={onClose} className="p-1 rounded-lg text-slate-400 hover:text-white hover:bg-slate-800 transition">
            <FaTimes className="w-5 h-5" />
          </button>
        </div>

        <div className="space-y-4 max-h-[70vh] overflow-y-auto pr-1">
          {/* Tone */}
          <div>
            <label className="block text-xs font-semibold uppercase tracking-wider text-slate-400 mb-1">Response Tone</label>
            <select
              value={preferences.preferred_tone || 'professional'}
              onChange={(e) => setPreferences({ ...preferences, preferred_tone: e.target.value })}
              className="w-full rounded-lg bg-slate-800 border border-slate-700 p-2.5 text-sm text-white focus:outline-none focus:border-indigo-500"
            >
              <option value="professional">Professional & Technical</option>
              <option value="concise">Concise & Direct</option>
              <option value="academic">Academic & In-depth</option>
              <option value="casual">Friendly & Casual</option>
            </select>
          </div>

          {/* Preferred Coding Language */}
          <div>
            <label className="block text-xs font-semibold uppercase tracking-wider text-slate-400 mb-1">Preferred Code Language</label>
            <select
              value={preferences.preferred_coding_language || 'Python'}
              onChange={(e) => setPreferences({ ...preferences, preferred_coding_language: e.target.value })}
              className="w-full rounded-lg bg-slate-800 border border-slate-700 p-2.5 text-sm text-white focus:outline-none focus:border-indigo-500"
            >
              <option value="Python">Python (PySpark / Data)</option>
              <option value="SQL">SQL (ANSI / T-SQL / Spark SQL)</option>
              <option value="Scala">Scala</option>
              <option value="TypeScript">TypeScript / JavaScript</option>
            </select>
          </div>

          {/* Response Depth */}
          <div>
            <label className="block text-xs font-semibold uppercase tracking-wider text-slate-400 mb-1">Explanation Depth</label>
            <select
              value={preferences.preferred_depth || 'deep_dive'}
              onChange={(e) => setPreferences({ ...preferences, preferred_depth: e.target.value })}
              className="w-full rounded-lg bg-slate-800 border border-slate-700 p-2.5 text-sm text-white focus:outline-none focus:border-indigo-500"
            >
              <option value="deep_dive">Deep Dive Architecture & Code</option>
              <option value="intermediate">Balanced Context & Examples</option>
              <option value="high_level">High-Level Executive Summary</option>
            </select>
          </div>

          {/* Custom Instructions */}
          <div>
            <label className="block text-xs font-semibold uppercase tracking-wider text-slate-400 mb-1">Custom System Instructions</label>
            <textarea
              value={preferences.custom_instructions || ''}
              onChange={(e) => setPreferences({ ...preferences, custom_instructions: e.target.value })}
              placeholder="e.g. Always include data validation steps in PySpark code..."
              rows={3}
              className="w-full rounded-lg bg-slate-800 border border-slate-700 p-2.5 text-sm text-white focus:outline-none focus:border-indigo-500 placeholder-slate-500"
            />
          </div>
        </div>

        <div className="flex items-center justify-between border-t border-slate-800 pt-4 mt-6">
          <span className="text-xs text-slate-400">Settings apply automatically to all conversations</span>
          <button
            onClick={handleSave}
            disabled={saving}
            className="flex items-center gap-2 px-4 py-2 bg-indigo-600 hover:bg-indigo-500 text-white rounded-lg font-medium text-sm transition shadow-lg shadow-indigo-600/30"
          >
            {savedSuccess ? (
              <>
                <FaCheck className="w-4 h-4 text-green-300" /> Saved!
              </>
            ) : (
              <>
                <FaSave className="w-4 h-4" /> Save Preferences
              </>
            )}
          </button>
        </div>

      </div>
    </div>
  );
};
