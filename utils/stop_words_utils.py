from datetime import datetime


def generate_year_stop_words(start_year=1850, end_year=None):
    """
    Generate a list of years as stop words
    """
    if end_year is None:
        end_year = datetime.now().year
    return [str(year) for year in range(start_year, end_year + 1)]


def generate_roman_numeral_stop_words(max_number=50):
    """
    Generate Roman numerals and common patterns
    """

    def int_to_roman(num):
        val = [1000, 900, 500, 400, 100, 90, 50, 40, 10, 9, 5, 4, 1]
        syb = ["M", "CM", "D", "CD", "C", "XC", "L", "XL", "X", "IX", "V", "IV", "I"]
        roman_num = ""
        i = 0
        while num > 0:
            for _ in range(num // val[i]):
                roman_num += syb[i]
                num -= val[i]
            i += 1
        return roman_num

    numerals = [int_to_roman(i) for i in range(1, max_number + 1)]
    # Add variations of each numeral (lowercase, etc.)
    tokenized_numerals = set()
    for numeral in numerals:
        tokenized_numerals.add(numeral.lower())  # Lowercase
        
        # Don't add phrases, just the key identifying words
        tokenized_numerals.add('team')
        tokenized_numerals.add('division')
        tokenized_numerals.add('div')
        tokenized_numerals.add('group')
        tokenized_numerals.add('serie')
        tokenized_numerals.add('liga')

    return list(tokenized_numerals)


def enhance_stop_words(base_stop_words):
    """
    Enhance base stop words with both years and Roman numerals
    """
    # Generate year-related stop words
    year_stop_words = generate_year_stop_words()
    year_related_terms = ["est", "established", "founded", "seit", "anno"]

    # Generate Roman numeral stop words
    roman_stop_words = generate_roman_numeral_stop_words()

    # Combine all stop words
    enhanced_stop_words = (
        base_stop_words + year_stop_words + year_related_terms + roman_stop_words
    )

    return list(enhanced_stop_words)
