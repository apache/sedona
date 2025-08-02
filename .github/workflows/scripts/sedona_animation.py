# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import os
import time
import sys

# Terminal escape codes for colors and cursor manipulation
# ANSI escape codes: https://en.wikipedia.org/wiki/ANSI_escape_code
CLEAR_SCREEN = "\033[2J"
HOME_CURSOR = "\033[H"
HIDE_CURSOR = "\033[?25l"
SHOW_CURSOR = "\033[?25h"
RESET_COLOR = "\033[0m"

# Basic ANSI colors
RED = "\033[91m"
GREEN = "\033[92m"
YELLOW = "\033[93m"
BLUE = "\033[94m"
MAGENTA = "\033[95m"
CYAN = "\033[96m"
WHITE = "\033[97m"

COLORS = [RED, GREEN, YELLOW, BLUE, MAGENTA, CYAN, WHITE]


def get_centered_text(text, terminal_width):
    """Calculates padding to center text."""
    padding = (terminal_width - len(text)) // 2
    return " " * max(0, padding) + text


def animate_sedona():
    """
    Plays a terminal animation based on "Apache Sedona" concepts.
    """
    message = "Apache Sedona"
    tagline = "Geospatial Big Data"
    processing_text = "Processing Data..."

    try:
        sys.stdout.write(HIDE_CURSOR)
        sys.stdout.flush()

        terminal_width = os.get_terminal_size().columns

        # Phase 1: Typing effect for "Apache Sedona"
        for i in range(len(message) + 1):
            current_message = message[:i]
            sys.stdout.write(CLEAR_SCREEN)
            sys.stdout.write(HOME_CURSOR)
            sys.stdout.write(
                get_centered_text(
                    f"{CYAN}{current_message}{RESET_COLOR}", terminal_width
                )
                + "\n"
            )
            sys.stdout.flush()
            time.sleep(0.1)

        time.sleep(0.5)

        # Phase 2: Pulsing tagline
        for _ in range(3):
            sys.stdout.write(CLEAR_SCREEN)
            sys.stdout.write(HOME_CURSOR)
            sys.stdout.write(
                get_centered_text(f"{CYAN}{message}{RESET_COLOR}", terminal_width)
                + "\n"
            )
            sys.stdout.write(
                get_centered_text(f"{YELLOW}{tagline}{RESET_COLOR}", terminal_width)
                + "\n"
            )
            sys.stdout.flush()
            time.sleep(0.3)
            sys.stdout.write(CLEAR_SCREEN)
            sys.stdout.write(HOME_CURSOR)
            sys.stdout.write(
                get_centered_text(f"{CYAN}{message}{RESET_COLOR}", terminal_width)
                + "\n"
            )
            sys.stdout.write(
                get_centered_text(
                    f"{RESET_COLOR}{tagline}{RESET_COLOR}", terminal_width
                )
                + "\n"
            )
            sys.stdout.flush()
            time.sleep(0.3)

        time.sleep(1)

        # Phase 3: Processing bar animation
        bar_length = 30
        for i in range(bar_length + 1):
            progress = i / bar_length
            filled_len = int(bar_length * progress)
            bar = "=" * filled_len + " " * (bar_length - filled_len)

            sys.stdout.write(CLEAR_SCREEN)
            sys.stdout.write(HOME_CURSOR)
            sys.stdout.write(
                get_centered_text(f"{CYAN}{message}{RESET_COLOR}", terminal_width)
                + "\n"
            )
            sys.stdout.write(
                get_centered_text(f"{YELLOW}{tagline}{RESET_COLOR}", terminal_width)
                + "\n\n"
            )
            sys.stdout.write(
                get_centered_text(f"{GREEN}[{bar}]{RESET_COLOR}", terminal_width) + "\n"
            )
            sys.stdout.write(
                get_centered_text(
                    f"{processing_text} {int(progress * 100)}%", terminal_width
                )
                + "\n"
            )
            sys.stdout.flush()
            time.sleep(0.08)

        time.sleep(1)

        # Phase 4: Final static display
        sys.stdout.write(CLEAR_SCREEN)
        sys.stdout.write(HOME_CURSOR)
        sys.stdout.write(
            get_centered_text(f"{CYAN}==================={RESET_COLOR}", terminal_width)
            + "\n"
        )
        sys.stdout.write(
            get_centered_text(f"{CYAN}  Apache Sedona  {RESET_COLOR}", terminal_width)
            + "\n"
        )
        sys.stdout.write(
            get_centered_text(f"{CYAN}==================={RESET_COLOR}", terminal_width)
            + "\n"
        )
        sys.stdout.write(
            get_centered_text(
                f"{GREEN}Data Processing Complete!{RESET_COLOR}", terminal_width
            )
            + "\n"
        )
        sys.stdout.flush()
        time.sleep(3)  # Display final message for a bit longer

    except KeyboardInterrupt:
        pass  # Allow graceful exit on Ctrl+C
    finally:
        sys.stdout.write(SHOW_CURSOR)
        sys.stdout.write(RESET_COLOR)
        sys.stdout.write(CLEAR_SCREEN)  # Clear screen on exit
        sys.stdout.write(HOME_CURSOR)
        sys.stdout.flush()


if __name__ == "__main__":
    animate_sedona()
