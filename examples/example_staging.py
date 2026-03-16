#!/usr/bin/env python3
"""
Example: File staging between client and edge.

Demonstrates:
- Listing remote directory contents
- Uploading a local file to the edge
- Downloading a remote file from the edge
- Error handling for existing files
"""

import os
import tempfile

from radical.edge import BridgeClient


def main():

    bc   = BridgeClient()
    eids = bc.list_edges()

    print(f"Found {len(eids)} Edge(s): {eids}")

    if not eids:
        print("No edges connected - start an edge service first")
        bc.close()
        return

    # Use first available edge
    ec      = bc.get_edge_client(eids[0])
    staging = ec.get_plugin('staging')

    print(f"\nConnected to edge: {eids[0]}")
    print("-" * 50)

    # Create a local test file
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as f:
        f.write("Hello from the client!\nThis is a test file for staging.")
        local_src = f.name

    print(f"Created local file: {local_src}")

    try:
        # List /tmp directory on edge
        print("\nListing /tmp on edge:")
        result = staging.list("/tmp")
        print(f"  Directory: {result['path']}")
        print(f"  Entries: {len(result['entries'])}")
        for entry in result['entries'][:5]:  # Show first 5
            etype = "📁" if entry['type'] == 'dir' else "📄"
            size = f"{entry['size']} bytes" if entry['size'] else ""
            print(f"    {etype} {entry['name']} {size}")
        if len(result['entries']) > 5:
            print(f"    ... and {len(result['entries']) - 5} more")

        # Upload to edge
        remote_path = f"/tmp/edge_staging_test_{os.getpid()}.txt"
        print(f"\nUploading to edge: {remote_path}")

        result = staging.put(local_src, remote_path)
        print(f"  -> Uploaded: {result['path']} ({result['size']} bytes)")

        # Download back to a different local path
        local_dst = local_src + ".downloaded"
        print(f"\nDownloading from edge: {remote_path}")

        result = staging.get(remote_path, local_dst)
        print(f"  -> Downloaded: {result['path']} ({result['size']} bytes)")

        # Verify content matches
        with open(local_src) as f:
            original = f.read()
        with open(local_dst) as f:
            downloaded = f.read()

        if original == downloaded:
            print("\nContent verification: PASSED")
        else:
            print("\nContent verification: FAILED")
            print(f"  Original:   {repr(original)}")
            print(f"  Downloaded: {repr(downloaded)}")

        # Demonstrate error handling: try to upload to same path again
        print(f"\nTrying to upload again to same path (should fail)...")
        try:
            staging.put(local_src, remote_path)
            print("  -> ERROR: Should have raised FileExistsError!")
        except FileExistsError as e:
            print(f"  -> Correctly raised FileExistsError: {e}")

        # Demonstrate error handling: try to download non-existent file
        print(f"\nTrying to download non-existent file (should fail)...")
        try:
            staging.get("/tmp/this_file_does_not_exist_12345.txt",
                        local_dst + ".missing")
            print("  -> ERROR: Should have raised FileNotFoundError!")
        except FileNotFoundError as e:
            print(f"  -> Correctly raised FileNotFoundError: {e}")

    finally:
        # Cleanup local files
        print("\nCleaning up local files...")
        if os.path.exists(local_src):
            os.unlink(local_src)
            print(f"  Removed: {local_src}")
        if os.path.exists(local_dst):
            os.unlink(local_dst)
            print(f"  Removed: {local_dst}")

    print("-" * 50)
    print("Done!")

    staging.close()
    bc.close()


if __name__ == "__main__":
    main()
