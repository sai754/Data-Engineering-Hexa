def capitalize_words(sentence):
    return sentence.title()

def reverse_string(s):
    return s[::-1]

def count_vowels(s):
    s= s.lower()
    c= 0
    for i in range(0,len(s)):
        if s[i] in 'aeiou':
            c+=1
    return c

def is_palindrome(s):
    return s == s[::-1]