"use client";

import React, { useState, useEffect } from 'react';
import {
  getConversations,
  createConversation,
  renameConversation,
  deleteConversation,
  searchChats,
  Conversation,
  SearchResult,
} from '@/lib/memory_api';
import { FaPlus, FaComment, FaTrash, FaEdit, FaCheck, FaSearch, FaBrain, FaSlidersH, FaTimes } from 'react-icons/fa';

interface ConversationSidebarProps {
  activeConversationId: string;
  onSelectConversation: (conversationId: string) => void;
  onOpenPreferences: () => void;
  onOpenMemories: () => void;
  userId?: string;
}

export const ConversationSidebar: React.FC<ConversationSidebarProps> = ({
  activeConversationId,
  onSelectConversation,
  onOpenPreferences,
  onOpenMemories,
  userId = 'default_user',
}) => {
  const [conversations, setConversations] = useState<Conversation[]>([]);
  const [editingId, setEditingId] = useState<string | null>(null);
  const [editingTitle, setEditingTitle] = useState('');
  const [searchQuery, setSearchQuery] = useState('');
  const [searchResults, setSearchResults] = useState<SearchResult[]>([]);
  const [isSearching, setIsSearching] = useState(false);

  const loadConversations = () => {
    getConversations(userId)
      .then(setConversations)
      .catch((err) => console.error('Failed to load conversations:', err));
  };

  useEffect(() => {
    loadConversations();
  }, [userId]);

  const handleNewChat = async () => {
    try {
      const newConv = await createConversation('New Chat', userId);
      setConversations((prev) => [newConv, ...prev]);
      onSelectConversation(newConv.conversation_id);
    } catch (err) {
      console.error('Failed to create new conversation:', err);
    }
  };

  const handleStartRename = (conv: Conversation, e: React.MouseEvent) => {
    e.stopPropagation();
    setEditingId(conv.conversation_id);
    setEditingTitle(conv.title);
  };

  const handleSaveRename = async (convId: string, e: React.FormEvent) => {
    e.preventDefault();
    if (!editingTitle.trim()) return;
    try {
      const updated = await renameConversation(convId, editingTitle.trim());
      setConversations((prev) => prev.map((c) => (c.conversation_id === convId ? updated : c)));
      setEditingId(null);
    } catch (err) {
      console.error('Failed to rename conversation:', err);
    }
  };

  const handleDelete = async (convId: string, e: React.MouseEvent) => {
    e.stopPropagation();
    if (!confirm('Are you sure you want to delete this chat?')) return;
    try {
      await deleteConversation(convId);
      setConversations((prev) => prev.filter((c) => c.conversation_id !== convId));
      if (activeConversationId === convId) {
        const remaining = conversations.filter((c) => c.conversation_id !== convId);
        if (remaining.length > 0) {
          onSelectConversation(remaining[0].conversation_id);
        } else {
          handleNewChat();
        }
      }
    } catch (err) {
      console.error('Failed to delete conversation:', err);
    }
  };

  const handleSearch = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!searchQuery.trim()) {
      setIsSearching(false);
      setSearchResults([]);
      return;
    }
    setIsSearching(true);
    try {
      const results = await searchChats(searchQuery, userId);
      setSearchResults(results);
    } catch (err) {
      console.error('Failed to search chats:', err);
    }
  };

  return (
    <div className="w-72 bg-slate-950 border-r border-slate-800 flex flex-col h-full text-slate-200">
      <div className="p-4 border-b border-slate-800 flex flex-col gap-3">
        <button
          onClick={handleNewChat}
          className="w-full flex items-center justify-center gap-2 py-2.5 px-4 bg-indigo-600 hover:bg-indigo-500 text-white rounded-xl font-medium shadow-lg shadow-indigo-600/20 transition"
        >
          <FaPlus className="w-5 h-5" /> New Chat
        </button>

        <form onSubmit={handleSearch} className="relative">
          <input
            type="text"
            placeholder="Search chats & memories..."
            value={searchQuery}
            onChange={(e) => {
              setSearchQuery(e.target.value);
              if (!e.target.value.trim()) setIsSearching(false);
            }}
            className="w-full bg-slate-900 border border-slate-800 rounded-lg py-1.5 pl-8 pr-3 text-xs text-slate-200 placeholder-slate-500 focus:outline-none focus:border-indigo-500"
          />
          <FaSearch className="w-3.5 h-3.5 text-slate-500 absolute left-2.5 top-2.5" />
          {searchQuery && (
            <button
              type="button"
              onClick={() => {
                setSearchQuery('');
                setIsSearching(false);
              }}
              className="absolute right-2 top-2 text-slate-500 hover:text-slate-300"
            >
              <FaTimes className="w-3.5 h-3.5" />
            </button>
          )}
        </form>
      </div>

      <div className="flex-1 overflow-y-auto p-2 space-y-1">
        {isSearching ? (
          <div>
            <div className="px-2 py-1 text-[11px] font-semibold uppercase tracking-wider text-slate-500">
              Search Results ({searchResults.length})
            </div>
            {searchResults.length === 0 ? (
              <p className="px-2 py-4 text-xs text-slate-500 text-center">No matching chats or messages found.</p>
            ) : (
              searchResults.map((res) => (
                <div
                  key={res.id}
                  onClick={() => {
                    if (res.conversation_id) {
                      onSelectConversation(res.conversation_id);
                      setIsSearching(false);
                    }
                  }}
                  className="p-2 rounded-lg hover:bg-slate-900 cursor-pointer text-xs space-y-1 transition border border-transparent hover:border-slate-800"
                >
                  <div className="flex items-center justify-between text-indigo-400 font-medium">
                    <span className="truncate">{res.title || res.result_type}</span>
                    <span className="text-[10px] text-slate-500">{(res.score * 100).toFixed(0)}%</span>
                  </div>
                  <p className="text-slate-400 line-clamp-2 text-[11px]">{res.content}</p>
                </div>
              ))
            )}
          </div>
        ) : (
          conversations.map((conv) => {
            const isActive = conv.conversation_id === activeConversationId;
            const isEditing = editingId === conv.conversation_id;

            return (
              <div
                key={conv.conversation_id}
                onClick={() => onSelectConversation(conv.conversation_id)}
                className={`group relative flex items-center justify-between p-2.5 rounded-xl cursor-pointer text-xs font-medium transition ${
                  isActive ? 'bg-indigo-600/15 text-indigo-300 border border-indigo-500/30' : 'hover:bg-slate-900 text-slate-400 hover:text-slate-200'
                }`}
              >
                <div className="flex items-center gap-2.5 overflow-hidden flex-1">
                  <FaComment className={`w-4 h-4 shrink-0 ${isActive ? 'text-indigo-400' : 'text-slate-500'}`} />

                  {isEditing ? (
                    <form onSubmit={(e) => handleSaveRename(conv.conversation_id, e)} className="flex-1">
                      <input
                        type="text"
                        value={editingTitle}
                        onChange={(e) => setEditingTitle(e.target.value)}
                        autoFocus
                        onBlur={(e) => handleSaveRename(conv.conversation_id, e)}
                        className="w-full bg-slate-800 text-white text-xs px-1.5 py-0.5 rounded focus:outline-none"
                      />
                    </form>
                  ) : (
                    <span className="truncate flex-1">{conv.title}</span>
                  )}
                </div>

                {!isEditing && (
                  <div className="opacity-0 group-hover:opacity-100 flex items-center gap-1 transition">
                    <button
                      onClick={(e) => handleStartRename(conv, e)}
                      title="Rename chat"
                      className="p-1 text-slate-400 hover:text-slate-200 hover:bg-slate-800 rounded transition"
                    >
                      <FaEdit className="w-3 h-3" />
                    </button>
                    <button
                      onClick={(e) => handleDelete(conv.conversation_id, e)}
                      title="Delete chat"
                      className="p-1 text-slate-400 hover:text-red-400 hover:bg-slate-800 rounded transition"
                    >
                      <FaTrash className="w-3 h-3" />
                    </button>
                  </div>
                )}
              </div>
            );
          })
        )}
      </div>

      <div className="p-3 border-t border-slate-800 flex items-center justify-between text-xs text-slate-400">
        <button
          onClick={onOpenMemories}
          className="flex items-center gap-1.5 py-1.5 px-2 rounded-lg hover:bg-slate-900 hover:text-purple-300 transition"
        >
          <FaBrain className="w-4 h-4 text-purple-400" /> Memories
        </button>
        <button
          onClick={onOpenPreferences}
          className="flex items-center gap-1.5 py-1.5 px-2 rounded-lg hover:bg-slate-900 hover:text-indigo-300 transition"
        >
          <FaSlidersH className="w-4 h-4 text-indigo-400" /> Preferences
        </button>
      </div>
    </div>
  );
};
