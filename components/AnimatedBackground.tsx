'use client';

import { motion, useReducedMotion } from 'framer-motion';
import { useRef, useState } from 'react';

interface AnimatedBackgroundProps {
  className?: string;
  onVideoEnd?: () => void;
  pauseTime?: number;
}

export default function AnimatedBackground({ className = '', onVideoEnd, pauseTime = 14 }: AnimatedBackgroundProps) {
  const reduceMotion = useReducedMotion();
  const videoRef = useRef<HTMLVideoElement>(null);
  const [hasTriggered, setHasTriggered] = useState(false);

  const handleTimeUpdate = (e: React.SyntheticEvent<HTMLVideoElement>) => {
    const video = e.currentTarget;
    if (video.currentTime >= pauseTime && !hasTriggered) {
      video.pause();
      video.currentTime = pauseTime;
      setHasTriggered(true);
      if (onVideoEnd) {
        onVideoEnd();
      }
    }
  };

  const handleEnded = () => {
    if (!hasTriggered) {
      setHasTriggered(true);
      if (onVideoEnd) {
        onVideoEnd();
      }
    }
  };

  return (
    <div className={`absolute inset-0 overflow-hidden ${className}`}>
      {/* Fallback Base Gradient */}
      <div
        aria-hidden
        className="absolute inset-0 bg-[#005a9c]"
        style={{
          background:
            'radial-gradient(circle at 60% 40%, #009cf5 0%, #005a9c 62%, #003660 100%)',
        }}
      />

      {/* Fullscreen Video Background */}
      <video
        ref={videoRef}
        autoPlay
        muted
        playsInline
        onTimeUpdate={handleTimeUpdate}
        onEnded={handleEnded}
        className="absolute inset-0 h-full w-full object-cover pointer-events-none"
      >
        <source src="/yeti-video.mp4" type="video/mp4" />
      </video>

      {/* Ambient orbs - Large glowing pure white/ice-blue wash at the bottom right */}
      <motion.div
        aria-hidden
        className="absolute -bottom-80 right-[-150px] h-[750px] w-[750px] rounded-full blur-[130px]"
        style={{
          background: 'radial-gradient(circle, rgba(255, 255, 255, 0.38) 0%, rgba(224, 242, 254, 0.15) 45%, transparent 75%)',
        }}
        initial={{ opacity: 0 }}
        animate={reduceMotion ? { x: 0, opacity: 0.85 } : { x: [0, -12, 0], opacity: [0, 0.7, 0.95, 0.7] }}
        transition={reduceMotion ? { duration: 0 } : { opacity: { times: [0, 0.02, 0.5, 1], duration: 24, repeat: Infinity, ease: 'easeInOut', delay: 0.5 }, x: { duration: 24, repeat: Infinity, ease: 'easeInOut', delay: 0.5 } }}
      />

      {/* Volumetric door light beam spilling from the right edge - cool pure white */}
      <motion.div
        aria-hidden
        className="absolute right-0 top-0 h-full w-[60vw] pointer-events-none blur-[65px]"
        style={{
          background: 'linear-gradient(280deg, rgba(255, 255, 255, 0.6) 0%, rgba(240, 249, 255, 0.35) 35%, rgba(224, 242, 254, 0.08) 65%, transparent 100%)',
          clipPath: 'polygon(100% 15%, 100% 85%, 0% 100%, 0% 0%)',
        }}
        initial={{ opacity: 0 }}
        animate={reduceMotion ? { opacity: 0.85 } : { opacity: [0.75, 0.88, 0.75] }}
        transition={reduceMotion ? { duration: 0 } : { duration: 12, repeat: Infinity, ease: 'easeInOut' }}
      />

      {/* Vignette + noise */}
      <div className="vignette-overlay absolute inset-0" aria-hidden />
      <div className="noise-overlay absolute inset-0" aria-hidden />
    </div>
  );
}
