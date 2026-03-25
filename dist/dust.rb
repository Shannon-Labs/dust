class Dust < Formula
  desc "Branchable local-first SQL database for AI agents"
  homepage "https://github.com/Shannon-Labs/dust"
  version "0.1.1"
  license "MIT"

  on_macos do
    if Hardware::CPU.arm?
      url "https://github.com/shannon-labs/dust/releases/download/v#{version}/dust-v#{version}-aarch64-apple-darwin.tar.gz"
      sha256 "PLACEHOLDER_ARM64_SHA256"
    else
      url "https://github.com/shannon-labs/dust/releases/download/v#{version}/dust-v#{version}-x86_64-apple-darwin.tar.gz"
      sha256 "PLACEHOLDER_X86_64_SHA256"
    end
  end

  on_linux do
    if Hardware::CPU.arm?
      url "https://github.com/shannon-labs/dust/releases/download/v#{version}/dust-v#{version}-aarch64-unknown-linux-gnu.tar.gz"
      sha256 "PLACEHOLDER_LINUX_ARM64_SHA256"
    else
      url "https://github.com/shannon-labs/dust/releases/download/v#{version}/dust-v#{version}-x86_64-unknown-linux-gnu.tar.gz"
      sha256 "PLACEHOLDER_LINUX_X86_64_SHA256"
    end
  end

  def install
    bin.install "dust"
  end

  test do
    system "#{bin}/dust", "--version"
  end
end
