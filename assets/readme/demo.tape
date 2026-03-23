Output assets/readme/demo.gif

Set Shell "bash"
Set FontSize 22
Set Width 1400
Set Height 900
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
Type "export PATH=$PWD/target/release:$PATH" Enter
Type "rm -rf /tmp/dust-readme-demo && mkdir -p /tmp/dust-readme-demo && cd /tmp/dust-readme-demo" Enter
Type "clear" Enter
Show

Type "dust init demo" Enter
Sleep 1s
Type "cd demo" Enter
Sleep 500ms

Hide
Type "dust query 'CREATE TABLE nums (id INTEGER PRIMARY KEY, value INTEGER); INSERT INTO nums (id, value) VALUES (1, 42), (2, 84)'" Enter
Show

Type "dust query 'SELECT * FROM nums ORDER BY id'" Enter
Sleep 1s
Type "dust branch create experiment" Enter
Sleep 1s
Type "dust branch list" Enter
Sleep 1s
Type "dust branch switch experiment" Enter
Sleep 1s
Type "dust branch list" Enter
Sleep 2s
