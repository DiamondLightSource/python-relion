# Copied from dlstbx.util.symlink

import os


def create_parent_symlink(
    destination_path, symlink_name, levels=2, overwrite_symlink=False
):
    """Create a symbolic link in a parent directory,
    $levels levels above the link destination.
    If a link already exists in that location it can be overwritten.
    If a file with the symlink name exists in the location it is left
    untouched.

    :param destination_path: The full path that is the symlink destination.
    :param symlink_name: The name of the symbolic link to be created.
    :param levels: The number of levels above the destination path where the
                   symlink should be created.
    :return: True if successful, False otherwise.
    """

    # Create symbolic link above working directory
    path_elements = destination_path.split(os.sep)

    # Full path to the symbolic link
    link_path = os.sep.join(path_elements[:-levels] + [symlink_name])

    # Only write symbolic link if a symbolic link is created or overwritten
    # Do not overwrite real files, do not touch real directories
    if not os.path.exists(link_path) or (
        overwrite_symlink and os.path.islink(link_path)
    ):
        # because symlinks can't be overwritten, create a temporary symlink in the
        # child directory and then rename on top of potentially existing one in
        # the parent directory.
        tmp_link = os.sep.join(path_elements[: -levels + 1] + [".tmp." + symlink_name])
        os.symlink(os.sep.join(path_elements[-levels:]), tmp_link)
        os.rename(tmp_link, link_path)
        return True
    return False
