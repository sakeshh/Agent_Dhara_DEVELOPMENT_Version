/**
 * API Client for Enterprise Conversation Memory System.
 */

export interface Conversation {
  conversation_id: string;
  user_id: string;
  title: string;
  status: string;
  is_deleted: boolean;
  created_at: string;
  updated_at: string;
  message_count?: number;
}

export interface Message {
  message_id: string;
  conversation_id: string;
  role: 'system' | 'user' | 'assistant';
  content: string;
  metadata?: Record<string, any>;
  timestamp: string;
}

export interface UserMemory {
  memory_id: string;
  user_id: string;
  memory_type: 'Profile' | 'Preference' | 'Skill' | 'Goal' | 'Project' | 'Interest';
  memory_text: string;
  confidence_score: number;
  source_conversation_id?: string;
  is_active: boolean;
  created_at: string;
  updated_at: string;
}

export interface UserPreferences {
  preference_id: string;
  user_id: string;
  preferred_language: string;
  preferred_coding_language: string;
  preferred_response_length: string;
  preferred_tone: string;
  preferred_depth: string;
  custom_instructions?: string;
  updated_at: string;
}

export interface SearchResult {
  result_type: 'message' | 'conversation' | 'memory' | 'summary';
  id: string;
  conversation_id?: string;
  title?: string;
  content: string;
  score: number;
  timestamp?: string;
}

const BACKEND_URL = process.env.NEXT_PUBLIC_API_URL || 'http://127.0.0.1:8000';

async function apiFetch<T>(endpoint: string, options: RequestInit = {}): Promise<T> {
  const res = await fetch(`${BACKEND_URL}${endpoint}`, {
    ...options,
    headers: {
      'Content-Type': 'application/json',
      ...(options.headers || {}),
    },
  });

  if (!res.ok) {
    const errorBody = await res.text().catch(() => '');
    throw new Error(`API Error ${res.status}: ${errorBody}`);
  }

  return res.json();
}

// Conversation Management
export async function getConversations(userId: string = 'default_user'): Promise<Conversation[]> {
  return apiFetch<Conversation[]>(`/api/v1/conversations?user_id=${encodeURIComponent(userId)}`);
}

export async function createConversation(title: string = 'New Chat', userId: string = 'default_user'): Promise<Conversation> {
  return apiFetch<Conversation>('/api/v1/conversations', {
    method: 'POST',
    body: JSON.stringify({ user_id: userId, title }),
  });
}

export async function renameConversation(conversationId: string, title: string): Promise<Conversation> {
  return apiFetch<Conversation>(`/api/v1/conversations/${conversationId}`, {
    method: 'PATCH',
    body: JSON.stringify({ title }),
  });
}

export async function deleteConversation(conversationId: string): Promise<{ ok: boolean }> {
  return apiFetch<{ ok: boolean }>(`/api/v1/conversations/${conversationId}`, {
    method: 'DELETE',
  });
}

export async function getMessages(conversationId: string): Promise<{ messages: Message[]; total_count: number }> {
  return apiFetch<{ messages: Message[]; total_count: number }>(`/api/v1/conversations/${conversationId}/messages`);
}

// Memories
export async function getUserMemories(userId: string = 'default_user'): Promise<UserMemory[]> {
  return apiFetch<UserMemory[]>(`/api/v1/memories?user_id=${encodeURIComponent(userId)}`);
}

export async function deleteUserMemory(memoryId: string): Promise<{ ok: boolean }> {
  return apiFetch<{ ok: boolean }>(`/api/v1/memories/${memoryId}`, {
    method: 'DELETE',
  });
}

// Preferences
export async function getUserPreferences(userId: string = 'default_user'): Promise<UserPreferences> {
  return apiFetch<UserPreferences>(`/api/v1/preferences?user_id=${encodeURIComponent(userId)}`);
}

export async function updateUserPreferences(prefs: Partial<UserPreferences>, userId: string = 'default_user'): Promise<UserPreferences> {
  return apiFetch<UserPreferences>(`/api/v1/preferences?user_id=${encodeURIComponent(userId)}`, {
    method: 'PUT',
    body: JSON.stringify(prefs),
  });
}

// Search
export async function searchChats(query: string, userId: string = 'default_user'): Promise<SearchResult[]> {
  return apiFetch<SearchResult[]>('/api/v1/search', {
    method: 'POST',
    body: JSON.stringify({ query, user_id: userId, search_type: 'hybrid' }),
  });
}

// Feedback
export async function submitFeedback(messageId: string, rating: number, comment?: string, userId: string = 'default_user') {
  return apiFetch('/api/v1/feedback', {
    method: 'POST',
    body: JSON.stringify({ message_id: messageId, rating, comment, user_id: userId }),
  });
}
