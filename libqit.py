import argparse
import collections
import configparser
import hashlib
import os
import re
import sys
import zlib

argparser = argparse.ArgumentParser(description="leo Qiao's implementation of gIT")
argsubparsers = argparser.add_subparsers(title="Commands", dest="command")
argsubparsers.required = True

def main(argv=sys.argv[1:]):
    args = argparser.parse_args(argv)

    if   args.command == "add"          : cmd_add(args)
    elif args.command == "cat-file"     : cmd_cat_file(args)
    elif args.command == "checkout"     : cmd_checkout(args)
    elif args.command == "commit"       : cmd_commit(args)
    elif args.command == "hash-object"  : cmd_hash_object(args)
    elif args.command == "init"         : cmd_init(args)
    elif args.command == "log"          : cmd_log(args)
    elif args.command == "ls-tree"      : cmd_ls_tree(args)
    elif args.command == "merge"        : cmd_merge(args)
    elif args.command == "rebase"       : cmd_rebase(args)
    elif args.command == "rev-parse"    : cmd_rev_parse(args)
    elif args.command == "rm"           : cmd_rm(args)
    elif args.command == "show-ref"     : cmd_show_ref(args)
    elif args.command == "tag"          : cmd_tag(args)


class GitRepository(object):
    """git repository"""

    worktree = None
    gitdir = None
    conf = None

    def __init__(self, path, force=False):
        self.worktree = path
        self.gitdir = os.path.join(path, ".git")
        
        if not (force or os.path.isdir(self.gitdir)):
            raise Exception("Not a Git repository %s" % path)

        # read configuration file in .git/config
        self.conf = configparser.ConfigParser()
        cf = repo_file(self, "config")

        if cf and os.path.exists(cf):
            self.conf.read([cf])
        elif not force:
            raise Exception("Configuration file missing")

        # check version
        if not force:
            vers = int(self.conf.get("core", "repositoryformatversion"))
            if vers != 0:
                raise Exception("Unsupported repositoryformatversion %s" % vers)

def repo_path(repo, *path):
    """Compute path under repo's gitdir."""
    return os.path.join(repo.gitdir, *path)

def repo_file(repo, *path, mkdir=False):
    """Same as repo_path, but create dirname(*path) if absent."""
    if repo_dir(repo, *path[:-1], mkdir=mkdir):
        return repo_path(repo, *path)

def repo_dir(repo, *path, mkdir=False):
    """Same as repo_path, but mkdir *path if absent if mkdir."""
    path = repo_path(repo, *path)

    if os.path.exists(path):
        if os.path.isdir(path):
            return path
        else:
            raise Exception("Not a directory %s" % path)

    if mkdir:
        os.makedirs(path)
        return path
    else:
        return None

def repo_create(path):
    """Create a new repository at path."""
    repo = GitRepository(path, True)

    # check that path either does not exist or is an empty dir
    if os.path.exists(repo.worktree):
        if not os.path.isdir(repo.worktree):
            raise Exception("%s is not a directory!" % path)
        if os.listdir(repo.worktree):
            raise Exception("%s is not empty!" % path)
    else:
        os.makedirs(repo.worktree)

    assert repo_dir(repo, "branches", mkdir=True)
    assert repo_dir(repo, "objects", mkdir=True)
    assert repo_dir(repo, "refs", "tags", mkdir=True)
    assert repo_dir(repo, "regs", "heads", mkdir=True)

    # .git/description
    with open(repo_file(repo, "description"), "w") as f:
        f.write("Unnamed repository; edit this file 'description' to name the repository.\n")

    # .git/HEAD
    with open(repo_file(repo, "HEAD"), "w") as f:
        f.write("ref: refs/heads/master\n")

    # .git/config
    with open(repo_file(repo, "config"), "w") as f:
        config = repo_default_config()
        config.write(f)

    return repo

def repo_default_config():
    ret = configparser.ConfigParser()

    ret.add_section("core")
    ret.set("core", "repositoryformatversion", "0")
    ret.set("core", "filemode", "false")
    ret.set("core", "bare", "false")

    return ret

# init
argsp = argsubparsers.add_parser("init", help="Initialize a new, empty repository.")
argsp.add_argument("path",
                   metavar="directory",
                   nargs="?",
                   default=".",
                   help="where to create the repository.")
def cmd_init(args):
    repo_create(args.path)

def repo_find(path=".", required=True):
    path = os.path.realpath(path)

    if os.path.isdir(os.path.join(path, ".git")):
        return GitRepository(path)

    parent = os.path.realpath(os.path.join(path, ".."))

    if parent == path:
        # base case:
        # os.path.join("/", "..") == "/"
        if required:
            raise Exception("No git repository")
        else:
            return None

    return repo_find(parent, required)

# objects
class GitObject(object):

    repo = None
    
    def __init__(self, repo, data=None):
        self.repo = repo

        if data != None:
            self.deserialize(data)

    def serialize(self):
        """This function must be implemented by subclasses"""
        raise Exception("Unimplemented!")

    def deserialize(self, data):
        """This function must be implemented by subclasses"""
        raise Exception("Unimplemented!")

def object_read(repo, sha):
    """Read object object_id from Git repository repo.
    Return a GitObject whose exact type depends on the object."""
    
    path = repo_file(repo, "objects", sha[:2], sha[2:])

    with open(path, "rb") as f:
        raw = zlib.decompress(f.read())

        # read object type
        x = raw.find(b' ')
        fmt = raw[:x]

        # read and validate object size
        y = raw.find(b'\x00', x)
        size = int(raw[x:y].decode("ascii"))
        if size != len(raw) - y - 1:
            raise Exception("Malformed object {0}: bad length".format(sha))

        # pick constructor
        if   fmt == b"commit" : c = GitCommit
        elif fmt == b"tree"   : c = GitTree
        elif fmt == b"tag"    : c = GitTag
        elif fmt == b"blob"   : c = GitBlob
        else:
            raise Exception("Unknown type {} for object {}".format(fmt.decode("ascii"), sha))

        return c(repo, raw[y+1:])

def object_find(repo, name, fmt=None, follow=True):
    sha = object_resolve(repo, name)

    if not sha:
        raise Exception("No such reference {0}.".format(name))
    
    if len(sha) > 1:
        raise Exception("Ambiguous referene {0}: Candidates are:\n - {1}".format(name, "\n - ".join(sha)))

    sha = sha[0]

    if not fmt:
        return sha

    while True:
        obj = object_read(repo, sha)

        if obj.fmt == fmt:
            return sha

        if not follow:
            return None

        # follow tags
        if obj.fmt == b"tag":
            sha = obj.kvlm[b"object"].decode("ascii")
        elif obj.fmt == b"commit" and fmt == b"tree":
            sha = obj.kvlm[b"tree"].decode("ascii")
        else:
            return None
    

def object_resolve(repo, name):
    """resolve name to an object hash in repo.
    - HEAD literal
    - short and long hashes
    - tags
    - branches
    - remote branches"""
    
    candidates = list()
    hashRE = re.compile(r"^[0-9A-Fa-f]{1,16}$")
    smallHashRE = re.compile(r"^[0-9A-Fa-f]{1,16}$")

    # empty string?
    if not name.strip():
        return None

    # HEAD
    if name == "HEAD":
        return [ ref_resolve(repo, "HEAD") ]

    if hashRE.match(name):
        if len(name) == 40:
            # complete hash
            return [ name.lower() ]
        elif len(name) >= 4:
            # minimal length for hash is 4
            name = name.lower()
            prefix = name[0:2]
            path = repo_dir(repo, "objects", prefix, mkdir=False)
            if path:
                rem = name[2:]
                for f in os.listdir(path):
                    if f.startswith(rem):
                        candidates.append(prefix + f)

    return candidates

def object_write(obj, actually_write=True):
    # serialize object data
    data = obj.serialize()
    # add header
    result = obj.fmt + b' ' + str(len(data)).encode("ascii") + b'\x00' + data
    # compute hash
    sha = hashlib.sha1(result).hexdigest()

    if actually_write:
        # compute path
        path = repo_file(obj.repo, "objects", sha[:2], sha[2:], mkdir=True)
        
        with open(path, "wb") as f:
            # compress and write
            f.write(zlib.compress(result))

    return sha

class GitBlob(GitObject):
    fmt = b"blob"

    def serialize(self):
        return self.blobdata

    def deserialize(self, data):
        self.blobdata = data

# cat-file
argsp = argsubparsers.add_parser("cat-file",
                                 help="provide content of repository objects")
argsp.add_argument("type",
                   metavar="type",
                   choices=["blob", "commit", "tag", "tree"],
                   help="specify the type")
argsp.add_argument("object",
                   metavar="object",
                   help="the object to display")

def cmd_cat_file(args):
    repo = repo_find()
    cat_file(repo, args.object, fmt=args.type.encode())

def cat_file(repo, obj, fmt=None):
    obj = object_read(repo, object_find(repo, obj, fmt=fmt))
    sys.stdout.buffer.write(obj.serialize())

# hash-object
argsp = argsubparsers.add_parser("hash-object",
                                 help="compute object ID and optionally creates a blob from a file")
argsp.add_argument("-t",
                   metavar="type",
                   dest="type",
                   choices=["blob", "commit", "tag", "tree"],
                   default="blob",
                   help="specify the type")
argsp.add_argument("-w",
                   dest="write",
                   action="store_true",
                   help="actually write the object into the database")
argsp.add_argument("path",
                   help="read object from <file>")

def cmd_hash_object(args):
    if args.write:
        repo = GitRepository(".")
    else:
        repo = None

    with open(args.path, "rb") as fd:
        sha = object_hash(fd, args.type.encode(), repo)
        print(sha)

def object_hash(fd, fmt, repo=None):
    data = fd.read()

    # choose constructor based on object type
    if   fmt==b'commit' : obj=GitCommit(repo, data)
    elif fmt==b'tree'   : obj=GitTree(repo, data)
    elif fmt==b'tag'    : obj=GitTag(repo, data)
    elif fmt==b'blob'   : obj=GitBlob(repo, data)
    else:
        raise Exception("Unknown type {}!".format(fmt))

    return object_write(obj, actually_write=bool(repo))

# commit object
def kvlm_parse(raw, start=0, dct=None):
    if not dct:
        dct = collections.OrderedDict()

    # search for space and newline
    spc = raw.find(b' ', start)
    nl = raw.find(b'\n', start)

    # base case:
    # if newline is before space (spc == -1), it is a blank line
    # then remainder is message
    if (spc < 0) or (nl < spc):
        assert nl == start
        dct[b''] = raw[start+1:]
        return dct
    
    # recursive case:
    # read a key-value pair and recurse
    key = raw[start:spc]
    
    # find a newline that is not followed by a space
    end = start
    while True:
        end = raw.find(b'\n', end+1)
        if raw[end+1] != ord(' '):
            break

    # drop the spaces
    value = raw[spc+1:end].replace(b"\n ", b"\n")

    # if key exists, append the value
    if key in dct:
        if type(dct[key]) == list:
            dct[key].append(value)
        else:
            dct[key] = [ dct[key], value ]
    else:
        dct[key] = value

    return kvlm_parse(raw, start=end+1, dct=dct)

def kvlm_serialize(kvlm):
    ret = b""

    # output fields
    for k in kvlm.keys():
        # skip the message for now
        if k == b"": continue

        val = kvlm[k]
        if type(val) != list:
            val = [ val ]

        for v in val:
            ret += k + b" " + (v.replace(b"\n", b"\n ")) + b"\n"

        # append message
        ret += b"\n" + kvlm[b""]

        return ret

class GitCommit(GitObject):
    fmt = b"commit"

    def deserialize(self, data):
        self.kvlm = kvlm_parse(data)

    def serialize(self):
        return kvlm_serialize(self.kvlm)

# log
argsp = argsubparsers.add_parser("log", help="display history of a given commit")
argsp.add_argument("commit",
                   default="HEAD",
                   nargs="?",
                   help="commit to start at")

def cmd_log(args):
    repo = repo_find()

    print("digraph qit{")
    log_graphviz(repo, object_find(repo, args.commit), set())
    print("}")

def log_graphviz(repo, sha, seen):
    if sha in seen:
        return
    seen.add(sha)

    commit = object_read(repo, sha)
    assert commit.fmt == b"commit"
    
    if not b"parent" in commit.kvlm.keys():
        # base case: initial commit
        return

    parents = commit.kvlm[b"parent"]

    if type(parents) != list:
        parents = [ parents ]

    for p in parents:
        p = p.decode("ascii")
        print("c_{0} -> c_{1};".format(sha, p))
        log_graphviz(repo, p, seen)

class GitTreeLeaf(object):
    def __init__(self, mode, path, sha):
        self.mode = mode
        self.path = path
        self.sha = sha

def tree_parse_one(raw, start=0):
    # find the space terminator of the mode
    x = raw.find(b" ", start)
    assert (x - start == 5) or (x - start == 6)

    # read the mode
    mode = raw[start:x]

    # find the NULL terminator of the path
    y = raw.find(b"\x00", x)
    path = raw[x+1:y]

    # read the SHA and convert to hex string
    # note: remove the '0x' at the beginning of hex()
    sha = hex(
            int.from_bytes(
                raw[y+1:y+21], "big"))[2:]

    return y+21, GitTreeLeaf(mode, path, sha)

def tree_parse(raw):
    pos = 0
    max = len(raw)
    ret = list()
    while pos < max:
        pos, data = tree_parse_one(raw, pos)
        ret.append(data)

    return ret

def tree_serialize(obj):
    ret = b""
    for i in obj.items:
        ret += i.mode
        ret += b" "
        ret += i.path
        ret += b"\x00"
        sha += int(i.sha, 16)
        ret += sha.to_bytes(20, byteorder="big")

    return ret

class GitTree(GitObject):
    fmt = b"tree"

    def serialize(self):
        return tree.serialize(self)

    def deserialize(self, data):
        self.items = tree_parse(data)

# ls-tree
argsp = argsubparsers.add_parser("ls-tree", help="pretty-print a tree object.")
argsp.add_argument("object", help="the object to show.")

def cmd_ls_tree(args):
    repo = repo_find()
    obj = object_read(repo, object_find(repo, args.object, fmt=b"tree"))

    for item in obj.items:
        print("{0} {1} {2}\t{3}".format(
            "0" * (6 - len(item.mode)) + item.mode.decode("ascii"),
            object_read(repo, item.sha).fmt.decode("ascii"),
            item.sha,
            item.path.decode("ascii")))

# checkout
argsp = argsubparsers.add_parser("checkout", help="checkout a commit inside of a directory.")
argsp.add_argument("commit",
                   help="the commit or tree to checkout.")
argsp.add_argument("path",
                   help="the EMPTY directory to checkout on.")

def cmd_checkout(args):
    repo = repo_find()

    obj = object_read(repo, object_find(repo, args.commit))

    # if the object is a commit, get its tree
    if obj.fmt == b"commit":
        obj = object_read(repo, obj.kvlm[b"tree"].decode("ascii"))

    # verify that path is an empty directory
    if os.path.exists(args.path):
        if not os.path.isdir(args.path):
            raise Exception("Not a directory {0}!".format(args.path))
        if os.listdir(args.path):
            raise Exception("Not empty {0}!".format(args.path))
    else:
        os.makedirs(args.path)

    tree_checkout(repo, obj, os.path.realpath(args.path).encode())

def tree_checkout(repo, tree, path):
    for item in tree.items:
        obj = object_read(repo, item.sha)
        dest = os.path.join(path, item.path)

        if obj.fmt == b"tree":
            os.mkdir(dest)
            tree_checkout(repo, obj, dest)
        elif obj.fmt == b"blob":
            with open(dest, "wb") as f:
                f.write(obj.blobdata)

def ref_resolve(repo, ref):
    with open(repo_file(repo, ref), "r") as fp:
        # discard the trailing newline
        data = fp.read()[:-1]
    if data.startswith("ref: "):
        return ref_resolve(repo, data[5:])
    else:
        return data

def ref_list(repo, path=None):
    if not path:
        path = repo_dir(repo, "refs")
    ret = collections.OrderedDict()
    # sort refs
    for f in sorted(os.listdir(path)):
        can = os.path.join(path,f)
        if os.path.isdir(can):
            ref[f] = ref_list(repo, can)
        else:
            ret[f] = ref_resolve(repo, can)

    return ret

# show-refs
argsp = argsubparsers.add_parser("show-ref", help="list references.")

def cmd_show_ref(args):
    repo = repo_find()
    refs = ref_list(repo)
    show_ref(repo, refs, prefix="refs")

def show_ref(repo, refs, with_hash=True, prefix=""):
    for k, v in refs.items():
        if type(v) == str:
            print("{0}{1}{2}".format(
                v + " " if with_hash else "",
                prefix + "/" if prefix else "",
                k))
        else:
            show_ref(repo, v, with_hash=with_hash, prefix="{0}{1}{2}".format(prefix, "/" if prefix else "", k))

# tags
class GitTag(GitCommit):
    fmt = b"tag"

argsp = argsubparsers.add_parser("tag", help="list and create tags.")
argsp.add_argument("-a",
                   action="store_true",
                   dest="create_tag_object",
                   help="whether to create a tag object")
argsp.add_argument("name",
                   nargs="?",
                   help="the new tag's name")
argsp.add_argument("object",
                   default="HEAD",
                   nargs="?",
                   help="the object the new tag will point to")

def cmd_tag(args):
    repo = repo_find()

    if args.name:
        tag_create(args.name,
                   args.object,
                   type="object" if args.create_tag_object else "ref")
    else:
        refs = ref_list(repo)
        show_ref(repo, refs["tags"], with_hash=False)

# rev-parse
argsp = argsubparsers.add_parser("rev-parse",
                                 help="parse revision (or other objects) identifiers")
argsp.add_argument("--qit-type",
                   metavar="type",
                   dest="type",
                   choices=["blob", "commit", "tag", "tree"],
                   default=None,
                   help="specify the expected type")
argsp.add_argument("name",
                   help="the name to parse")

def cmd_rev_parse(args):
    if args.type:
        fmt = args.type.encode()

    repo = repo_find()

    print(object_find(repo, args.name, args.type, follow=True))
