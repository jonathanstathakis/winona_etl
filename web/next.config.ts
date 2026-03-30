import type { NextConfig } from "next";

const apiUrl = process.env.API_URL ?? "http://localhost:8000";

const nextConfig: NextConfig = {
  experimental: {
    proxyClientMaxBodySize: 100 * 1024 * 1024, // 100MB
  },
  async rewrites() {
    return [
      {
        source: "/api/:path*",
        destination: `${apiUrl}/api/:path*`,
      },
    ];
  },
};

export default nextConfig;
