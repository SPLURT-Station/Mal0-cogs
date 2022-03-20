import yaml, os, datetime, pprint

def change_to_tuples(dict):
    """
    Turns an unitary dictionary into a tuple
    """
    return (list(dict.keys())[0], list(dict.values())[0])

class RepoError(Exception):
    pass

def readCl(instancePath, dateOverride):
    """
    Used for reading and organizing the yaml Changelog Files
    """
    dateOverride = datetime.datetime.strptime(dateOverride, '%Y-%m-%d')
    archiveDir = os.path.join(os.path.abspath(instancePath), "Repository/html/changelogs/archive")
    if not os.path.exists(archiveDir):
        raise RepoError("There was a problem while browsing the repository")
    formattedDate = dateOverride.strftime("%Y-%m")
    monthPath = os.path.join(archiveDir, formattedDate + ".yml")
    currentEntries = {}
    if not os.path.exists(monthPath):
        raise AttributeError
    with open(monthPath, 'r', encoding='utf-8') as f:
        currentEntries = yaml.load(f, Loader=yaml.SafeLoader)
    return createChanges(currentEntries, dateOverride)

def createChanges(cl, dateOverride):
    """
    Generates a dict that can be later turned into a formatted embed
    """
    date_cl = cl.get(dateOverride.date())
    changes = {}
    numCh = 0
    for author, tags in date_cl.items():
        numCh += 1
        if not author in changes.keys():
            changes.update({author : {}})
        for tag in tags:
            ch = change_to_tuples(tag)
            if ch[0] in changes.get(author).keys():
                changes[author][ch[0]].append(ch[1])
            else:
                changes[author].update({ch[0] : [ch[1]]})
    return changes, numCh

def main():
    when = ""
    how = False
    if len(when):
        how = True
    print(when, len(when), how)

if __name__ == "__main__":
    main()
