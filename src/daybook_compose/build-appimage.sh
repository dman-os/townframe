#!/bin/bash
# Script to build an AppImage using jpackage output and appimagetool from nix
# Uses createReleaseDistributable output and converts it to AppImage

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Build the distributable using Gradle
echo "Building distributable with Gradle..."
./gradlew createReleaseDistributable

# Define paths
DIST_DIR="composeApp/build/compose/binaries/main-release/app/org.example.daybook"
OUTPUT="composeApp/build/compose/binaries/main-release/app/org.example.daybook-x86_64.AppImage"

# Check if appimagetool is available from nix
if ! command -v appimagetool &> /dev/null; then
    echo "Error: appimagetool not found in PATH"
    echo "Please ensure you're in a nix shell with appimagetools:"
    echo "  nix develop .#ci-desktop  # or .#dev"
    exit 1
fi

# Ensure AppRun exists
if [ ! -f "$DIST_DIR/AppRun" ]; then
    echo "Creating AppRun script..."
    cat > "$DIST_DIR/AppRun" << 'EOF'
#!/bin/bash
HERE="$(dirname "$(readlink -f "${0}")")"
exec "${HERE}/bin/org.example.daybook" "$@"
EOF
    chmod +x "$DIST_DIR/AppRun"
fi

# Ensure .desktop file exists
if [ ! -f "$DIST_DIR/org.example.daybook.desktop" ]; then
    echo "Creating .desktop file..."
    cat > "$DIST_DIR/org.example.daybook.desktop" << 'EOF'
[Desktop Entry]
Type=Application
Name=Daybook
Comment=Daybook Desktop Application
Exec=org.example.daybook
Icon=org.example.daybook
Categories=Utility;
EOF
fi

# Copy icon to AppDir root if needed
if [ ! -f "$DIST_DIR/org.example.daybook.png" ] && [ -f "$DIST_DIR/lib/org.example.daybook.png" ]; then
    echo "Copying icon to AppDir root..."
    cp "$DIST_DIR/lib/org.example.daybook.png" "$DIST_DIR/org.example.daybook.png"
fi

# Build the AppImage using appimagetool from nix
echo "Creating AppImage with appimagetool..."
unset SOURCE_DATE_EPOCH
appimagetool "$DIST_DIR" "$OUTPUT"

echo ""
echo "✓ AppImage created successfully!"
echo "  Location: $OUTPUT"
echo "  Size: $(du -h "$OUTPUT" | cut -f1)"
