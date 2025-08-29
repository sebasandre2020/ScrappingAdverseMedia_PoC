def normalize_string(input_string):
    if not input_string:
        return ""
    
    return input_string.strip()

def normalize_string_special_chars(input_string):
    if not input_string:
        return ""
    
    special_cases = {
        # Symbols and placeholders
        "-", ".", "+", "*", "~", "...", "?", "—",  

        # English null-like values
        "empty", "null", "none", "n/a", "N/A", "NA", "na", "nil", "Nil", "NULL", "None",  
        "void", "undefined", "missing", "not available",  

        # Spanish null-like values
        "vacío", "nulo", "ninguno", "no disponible", "desconocido", "sin especificar",  
        "no aplica", "N/A", "ND", "No Definido", "sin dato", "s/d", "sin información"
    }
    
    trimmed = input_string.strip()
    if len(trimmed) < 3 or trimmed.lower() in special_cases:
        return ""

    return trimmed
