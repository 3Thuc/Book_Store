import React, { useState, useEffect } from 'react';

// Reusable LoadingScreen / PageLoader component used across the app.
const LoadingScreen: React.FC = () => {
  const [progress, setProgress] = useState(0);

  useEffect(() => {
    // Simulate loading progress from 0 to 100
    const duration = 1500; // 1.5 seconds total
    const steps = 60; // 60 steps for smooth animation
    const increment = 100 / steps;
    const intervalTime = duration / steps;

    const interval = setInterval(() => {
      setProgress((prev) => {
        const next = prev + increment;
        if (next >= 100) {
          clearInterval(interval);
          return 100;
        }
        return next;
      });
    }, intervalTime);

    return () => clearInterval(interval);
  }, []);

  return (
    <div className="loading-screen">
      <div className="loading-icon">
        <svg
          width="48"
          height="48"
          viewBox="0 0 24 24"
          fill="none"
          stroke="currentColor"
          strokeWidth="2"
          strokeLinecap="round"
          strokeLinejoin="round"
          className="text-foreground"
        >
          <path d="M4 19.5v-15A2.5 2.5 0 0 1 6.5 2H20v20H6.5a2.5 2.5 0 0 1 0-5H20" />
        </svg>
      </div>
      <div className="loading-progress-container">
        <div
          className="loading-progress-bar"
          style={{ width: `${progress}%` }}
        />
      </div>
    </div>
  );
};

const PageLoader: React.FC = () => <LoadingScreen />;

export default PageLoader;
