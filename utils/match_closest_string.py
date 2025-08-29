import difflib

def match_closest_string(reference, option1, option2):
    matcher1 = difflib.SequenceMatcher(None, reference, option1)
    matcher2 = difflib.SequenceMatcher(None, reference, option2)
    
    similarity1 = matcher1.ratio()
    similarity2 = matcher2.ratio()
    
    if similarity1 > similarity2:
        return option1
    else:
        return option2
