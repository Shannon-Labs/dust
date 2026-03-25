Output assets/readme/demo.gif

Set Shell "bash"
Set FontSize 22
Set Width 1400
Set Height 1040
Set Padding 24
Set Framerate 24
Set PlaybackSpeed 1.1
Set TypingSpeed 40ms
Set BorderRadius 12
Set WindowBar Colorful
Set Theme "GitHub Dark"
Set Margin 12
Set MarginFill "#0d1117"

Require bash

Hide
Type "export PATH=$PWD/target/debug:$PATH" Enter
Type "rm -rf /tmp/dust-readme-demo" Enter
Type "clear" Enter
Show

Type "dust demo --delay-ms 500 /tmp/dust-readme-demo" Enter
Sleep 2s
