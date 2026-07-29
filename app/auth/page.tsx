'use client';

import { useEffect, useState } from 'react';
import { motion } from 'framer-motion';
import { useRouter, useSearchParams } from 'next/navigation';
import { FaEye, FaEyeSlash, FaEnvelope, FaLock, FaArrowLeft, FaUser } from 'react-icons/fa';
import AnimatedBackground from '@/components/AnimatedBackground';

export default function AuthPage() {
  const router = useRouter();
  const searchParams = useSearchParams();
  const [activeTab, setActiveTab] = useState<'login' | 'signup'>('login');
  const [showPassword, setShowPassword] = useState(false);
  const [showConfirmPassword, setShowConfirmPassword] = useState(false);
  const [emailError, setEmailError] = useState<string | null>(null);
  const [passwordError, setPasswordError] = useState<string | null>(null);
  const [confirmPasswordError, setConfirmPasswordError] = useState<string | null>(null);
  const [formData, setFormData] = useState({
    firstName: '',
    lastName: '',
    email: '',
    password: '',
    confirmPassword: '',
  });

  const isCapgeminiEmail = (email: string) => {
    const trimmed = email.trim().toLowerCase();
    return trimmed.endsWith('@capgemini.com') && trimmed.length > '@capgemini.com'.length;
  };

  const validatePassword = (password: string, email: string) => {
    const pwd = password ?? '';
    const username = (email.split('@')[0] ?? '').trim().toLowerCase();

    if (pwd.length < 8) return 'Password must be at least 8 characters.';
    if (!/[A-Z]/.test(pwd)) return 'Password must include at least 1 capital letter.';
    if (!/[0-9]/.test(pwd)) return 'Password must include at least 1 number.';
    if (!/[^a-zA-Z0-9]/.test(pwd)) return 'Password must include at least 1 special character (e.g., @, $, !).';
    if (username && pwd.toLowerCase().includes(username))
      return 'Password must not contain your username (email prefix).';
    return null;
  };

  useEffect(() => {
    const tab = searchParams.get('tab');
    if (tab === 'signup' || tab === 'login') setActiveTab(tab);
  }, [searchParams]);

  useEffect(() => {
    if (!formData.email) {
      setEmailError(null);
      return;
    }
    setEmailError(isCapgeminiEmail(formData.email) ? null : 'Please use your @capgemini.com email.');
  }, [formData.email]);

  useEffect(() => {
    if (!formData.password) {
      setPasswordError(null);
      return;
    }
    setPasswordError(validatePassword(formData.password, formData.email));
  }, [formData.password, formData.email]);

  useEffect(() => {
    if (activeTab !== 'signup') {
      setConfirmPasswordError(null);
      return;
    }
    if (!formData.confirmPassword) {
      setConfirmPasswordError(null);
      return;
    }
    setConfirmPasswordError(
      formData.confirmPassword === formData.password ? null : 'Passwords do not match.'
    );
  }, [activeTab, formData.confirmPassword, formData.password]);

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (!isCapgeminiEmail(formData.email)) {
      setEmailError('Please use your @capgemini.com email.');
      return;
    }
    const pwdErr = validatePassword(formData.password, formData.email);
    if (pwdErr) {
      setPasswordError(pwdErr);
      return;
    }
    if (activeTab === 'signup' && formData.confirmPassword !== formData.password) {
      setConfirmPasswordError('Passwords do not match.');
      return;
    }
    router.push('/chat');
  };

  return (
    <div className="relative min-h-screen overflow-hidden bg-transparent">
      <AnimatedBackground pauseTime={7} />

      {/* Back Button */}
      <motion.button
        onClick={() => router.push('/')}
        className="absolute top-6 left-6 flex items-center gap-2 rounded-full border border-white/10 bg-white/10 px-4 py-2 text-white/80 backdrop-blur transition-colors hover:bg-white/20 hover:text-white"
        initial={{ opacity: 0, x: -20 }}
        animate={{ opacity: 1, x: 0 }}
        transition={{ duration: 0.5 }}
      >
        <FaArrowLeft />
        <span className="font-medium">Back</span>
      </motion.button>

      {/* Main Content */}
      <div className="flex items-center justify-center md:justify-start min-h-screen px-6 py-12 md:pl-24 lg:pl-32">
        <motion.div
          className="w-full max-w-md"
          initial={{ opacity: 0, y: 30 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.6 }}
        >
          {/* Card */}
          <div className="relative overflow-hidden rounded-3xl border border-white/25 bg-white/20 shadow-[0_30px_120px_rgba(0,0,0,0.25)] backdrop-blur-2xl">
            <div className="absolute inset-0 bg-gradient-to-br from-[#12ABDB]/15 via-transparent to-white/10" />
            {/* Header */}
            <div className="relative p-8 pb-6">
              <motion.h2
                className="text-2xl md:text-[26px] font-extrabold text-center tracking-tight text-white [font-family:Helvetica,Arial,sans-serif] mb-2 leading-tight text-balance px-2"
                initial={{ opacity: 0 }}
                animate={{ opacity: 1 }}
                transition={{ delay: 0.2 }}
              >
                Welcome to AGENT DHARA
              </motion.h2>
              <p className="text-center text-white/70 text-sm">
                {activeTab === 'login' ? 'Sign in to continue' : 'Create your account'}
              </p>
            </div>

            {/* Tab Switcher */}
            <div className="relative px-8 pb-2">
              <div className="grid grid-cols-2 rounded-2xl border border-zinc-200 bg-white p-1 shadow-sm">
              <button
                onClick={() => setActiveTab('login')}
                className={`relative rounded-xl py-3 text-center text-sm font-semibold transition-all duration-300 ${
                  activeTab === 'login'
                    ? 'text-white'
                    : 'text-zinc-500 hover:text-[#0070AD]'
                }`}
              >
                {activeTab === 'login' && (
                  <motion.div
                    layoutId="activeTab"
                    className="absolute inset-0 rounded-xl bg-gradient-to-r from-[#12ABDB] to-[#0070AD] shadow-[0_4px_12px_rgba(0,112,173,0.15)]"
                    transition={{ type: 'spring', stiffness: 500, damping: 30 }}
                  />
                )}
                <span className="relative z-10">Login</span>
              </button>
              <button
                onClick={() => setActiveTab('signup')}
                className={`relative rounded-xl py-3 text-center text-sm font-semibold transition-all duration-300 ${
                  activeTab === 'signup'
                    ? 'text-white'
                    : 'text-zinc-500 hover:text-[#0070AD]'
                }`}
              >
                {activeTab === 'signup' && (
                  <motion.div
                    layoutId="activeTab"
                    className="absolute inset-0 rounded-xl bg-gradient-to-r from-[#12ABDB] to-[#0070AD] shadow-[0_4px_12px_rgba(0,112,173,0.15)]"
                    transition={{ type: 'spring', stiffness: 500, damping: 30 }}
                  />
                )}
                <span className="relative z-10">Sign up</span>
              </button>
              </div>
            </div>

            {/* Form */}
            <div className="relative p-8">
              <form onSubmit={handleSubmit} className="space-y-5">
                {/* Name fields (Signup only) */}
                {activeTab === 'signup' && (
                  <motion.div
                    initial={{ opacity: 0, y: 16 }}
                    animate={{ opacity: 1, y: 0 }}
                    transition={{ duration: 0.4, delay: 0.15 }}
                    className="grid grid-cols-1 gap-4 sm:grid-cols-2"
                  >
                    <div>
                      <label className="block text-sm font-medium text-white/80 mb-2">
                        First name
                      </label>
                      <div className="relative">
                        <FaUser className="absolute left-4 top-1/2 -translate-y-1/2 text-zinc-400" />
                        <input
                          type="text"
                          value={formData.firstName}
                          onChange={(e) => setFormData({ ...formData, firstName: e.target.value })}
                          placeholder="First name"
                          autoComplete="given-name"
                          className="w-full rounded-2xl border border-zinc-200 bg-white px-4 py-3 pl-12 text-zinc-900 placeholder-zinc-400 outline-none transition-all focus:ring-2 focus:border-sky-500 focus:ring-sky-500/20"
                          required={activeTab === 'signup'}
                        />
                      </div>
                    </div>

                    <div>
                      <label className="block text-sm font-medium text-white/80 mb-2">
                        Last name
                      </label>
                      <div className="relative">
                        <FaUser className="absolute left-4 top-1/2 -translate-y-1/2 text-zinc-400" />
                        <input
                          type="text"
                          value={formData.lastName}
                          onChange={(e) => setFormData({ ...formData, lastName: e.target.value })}
                          placeholder="Last name"
                          autoComplete="family-name"
                          className="w-full rounded-2xl border border-zinc-200 bg-white px-4 py-3 pl-12 text-zinc-900 placeholder-zinc-400 outline-none transition-all focus:ring-2 focus:border-sky-500 focus:ring-sky-500/20"
                          required={activeTab === 'signup'}
                        />
                      </div>
                    </div>
                  </motion.div>
                )}

                {/* Email Field */}
                <motion.div
                  initial={{ opacity: 0, y: 16 }}
                  animate={{ opacity: 1, y: 0 }}
                  transition={{ duration: 0.4, delay: 0.2 }}
                >
                  <label className="block text-sm font-medium text-white/80 mb-2">
                    Email
                  </label>
                  <div className="relative">
                    <FaEnvelope className="absolute left-4 top-1/2 -translate-y-1/2 text-zinc-400" />
                    <input
                      type="email"
                      value={formData.email}
                      onChange={(e) => setFormData({ ...formData, email: e.target.value })}
                      placeholder="user@capgemini.com"
                      inputMode="email"
                      autoComplete="email"
                      className={`w-full rounded-2xl border px-4 py-3 pl-12 text-zinc-900 placeholder-zinc-400 outline-none transition-all focus:ring-2 ${
                        emailError
                          ? 'border-red-300 bg-red-50/95 focus:border-red-500 focus:ring-red-500/20'
                          : 'border-zinc-200 bg-white focus:border-sky-500 focus:ring-sky-500/20'
                      }`}
                      required
                    />
                  </div>
                  {emailError && (
                    <p className="mt-2 text-sm text-red-300/90">
                      {emailError}
                    </p>
                  )}
                </motion.div>

                {/* Password Field */}
                <motion.div
                  initial={{ opacity: 0, y: 16 }}
                  animate={{ opacity: 1, y: 0 }}
                  transition={{ duration: 0.4, delay: 0.3 }}
                >
                  <label className="block text-sm font-medium text-white/80 mb-2">
                    Password
                  </label>
                  <div className="relative">
                    <FaLock className="absolute left-4 top-1/2 -translate-y-1/2 text-zinc-400" />
                    <input
                      type={showPassword ? 'text' : 'password'}
                      value={formData.password}
                      onChange={(e) => setFormData({ ...formData, password: e.target.value })}
                      placeholder="••••••••"
                      className={`w-full rounded-2xl border px-4 py-3 pl-12 pr-12 text-zinc-900 placeholder-zinc-400 outline-none transition-all focus:ring-2 ${
                        passwordError
                          ? 'border-red-300 bg-red-50/95 focus:border-red-500 focus:ring-red-500/20'
                          : 'border-zinc-200 bg-white focus:border-sky-500 focus:ring-sky-500/20'
                      }`}
                      required
                    />
                    <button
                      type="button"
                      onClick={() => setShowPassword(!showPassword)}
                      className="absolute right-4 top-1/2 -translate-y-1/2 text-zinc-400 hover:text-zinc-650 transition-colors"
                    >
                      {showPassword ? <FaEyeSlash /> : <FaEye />}
                    </button>
                  </div>
                  {passwordError && (
                    <p className="mt-2 text-sm text-red-300/90">
                      {passwordError}
                    </p>
                  )}
                </motion.div>

                {/* Confirm Password Field (Signup only) */}
                {activeTab === 'signup' && (
                  <motion.div
                    initial={{ opacity: 0, y: 16, height: 0 }}
                    animate={{ opacity: 1, y: 0, height: 'auto' }}
                    exit={{ opacity: 0, height: 0 }}
                    transition={{ duration: 0.4, delay: 0.1 }}
                  >
                    <label className="block text-sm font-medium text-white/80 mb-2">
                      Confirm Password
                    </label>
                    <div className="relative">
                      <FaLock className="absolute left-4 top-1/2 -translate-y-1/2 text-zinc-400" />
                      <input
                        type={showConfirmPassword ? 'text' : 'password'}
                        value={formData.confirmPassword}
                        onChange={(e) =>
                          setFormData({ ...formData, confirmPassword: e.target.value })
                        }
                        placeholder="••••••••"
                        className={`w-full rounded-2xl border px-4 py-3 pl-12 pr-12 text-zinc-900 placeholder-zinc-400 outline-none transition-all focus:ring-2 ${
                          confirmPasswordError
                            ? 'border-red-300 bg-red-50/95 focus:border-red-500 focus:ring-red-500/20'
                            : 'border-zinc-200 bg-white focus:border-sky-500 focus:ring-sky-500/20'
                        }`}
                        required={activeTab === 'signup'}
                      />
                      <button
                        type="button"
                        onClick={() => setShowConfirmPassword(!showConfirmPassword)}
                        className="absolute right-4 top-1/2 -translate-y-1/2 text-zinc-400 hover:text-zinc-600 transition-colors"
                      >
                        {showConfirmPassword ? <FaEyeSlash /> : <FaEye />}
                      </button>
                    </div>
                    {confirmPasswordError && (
                      <p className="mt-2 text-sm text-red-300/90">
                        {confirmPasswordError}
                      </p>
                    )}
                  </motion.div>
                )}

                {/* Submit Button */}
                <motion.div
                  initial={{ opacity: 0, y: 16 }}
                  animate={{ opacity: 1, y: 0 }}
                  transition={{ duration: 0.4, delay: 0.4 }}
                >
                <motion.button
                  type="submit"
                  disabled={
                    !!emailError ||
                    !formData.email ||
                    !formData.password ||
                    !!passwordError ||
                    (activeTab === 'signup' && !!confirmPasswordError)
                  }
                  whileHover={{ scale: 1.02 }}
                  whileTap={{ scale: 0.98 }}
                  className={`group relative mt-6 w-full overflow-hidden rounded-2xl border py-3 font-semibold backdrop-blur transition-all duration-300 ${
                    emailError ||
                    !formData.email ||
                    !formData.password ||
                    passwordError ||
                    (activeTab === 'signup' && confirmPasswordError)
                      ? 'cursor-not-allowed border-white/5 bg-white/5 text-white/30 shadow-none'
                      : 'border-white/20 bg-white text-[#005a9c] shadow-[0_18px_60px_rgba(255,255,255,0.05)] hover:bg-white/90 hover:text-[#005a9c] hover:shadow-[0_18px_80px_rgba(255,255,255,0.1)]'
                  }`}
                >
                  <span className="relative z-10">{activeTab === 'login' ? 'Sign In' : 'Create Account'}</span>
                  {!emailError && !passwordError && formData.email && formData.password && (
                    <span className="absolute inset-0 rounded-2xl bg-gradient-to-r from-sky-400 to-[#12ABDB] opacity-0 transition-opacity duration-300 group-hover:opacity-100" />
                  )}
                </motion.button>
                </motion.div>
              </form>

              {/* Forgot Password (Login only) */}
              {activeTab === 'login' && (
                <div className="mt-4 text-center">
                  <button className="text-sm text-white/60 hover:text-white/80 font-medium transition-colors">
                    Forgot password?
                  </button>
                </div>
              )}
            </div>
          </div>

          {/* Footer Text */}
          <p className="text-center text-white/60 text-sm mt-6">
            By continuing, you agree to our Terms of Service and Privacy Policy
          </p>
        </motion.div>
      </div>
    </div>
  );
}
