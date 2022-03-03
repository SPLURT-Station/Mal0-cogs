import yaml, os, glob

def reader(instancePath):
    """
    Used for reading and organizing the yaml files
    """

    if not os.path.exists(instancePath):
        return
    clPath = os.path.join(os.path.abspath(instancePath), "Repository/html/changelogs")
    currentCl = {}
    numCl = 0
    for fileName in glob.glob(os.path.join(clPath, "*.yml")):
        name = os.path.splitext(os.path.basename(fileName))
        if name[0].startswith('.'): continue
        if name[0] == 'example': continue
        cl = {}
        numCl += 1
        with open(fileName, 'r', encoding='utf-8') as f:
            cl = yaml.load(f, Loader=yaml.SafeLoader)
        changes = cl.get('changes')
        newChanges = {}
        for ch in changes:
            for k, v in ch.items():
                if k in newChanges:
                    newChanges[k] += [v]
                else:
                    newChanges.update({k: [v]})
        if cl.get('author') in currentCl:
            for k, v in newChanges.items():
                if k in currentCl.get(cl.get('author')):
                    currentCl[cl.get('author')].update({k: currentCl.get(cl.get('author')).get(k) + v})
                else:
                    currentCl[cl.get('author')].update({k: v})
        else:
            currentCl.update({cl['author']: newChanges})
    return (numCl, currentCl)
