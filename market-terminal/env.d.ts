/// <reference types="vite/client" />

interface Window {
  terminal?: {
    platform: string
    versions: {
      electron: string
      chrome: string
      node: string
    }
  }
}
