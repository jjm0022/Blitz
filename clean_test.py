import clean
import unittest


class TestClean(unittest.TestCase):

    def test_remove_new_line_char(self):
        test_string = 'This is a \n test\n'
        string = clean.remove_new_line_char(test_string)
        self.assertEqual(string, 'This is a   test ')

    def test_remove_bad_characters(self):
        test_string = ' This += little . 5" % t4t #.!, '
        string = clean.remove_bad_characters(test_string)
        self.assertEqual(string, ' This    little .      t t  .!, ')

    def test_remove_multiple_periods(self):
        test_string = 'This little light... of mine ... .'
        string = clean.remove_multiple_periods(test_string)
        self.assertEqual(string, 'This little light  of mine   .')

    def test_remove_words_numbers(self):
        test_string = 'TH15 L1TT13 l1ght O44 m1n3'
        string = clean.remove_words_letters_numbers(test_string)
        self.assertEqual(string, '  L1TT13 l1ght   m1n3')

    def test_remove_multiple_spaces(self):
        test_string = 'This  Little light of   mine         '
        string = clean.remove_multiple_spaces(test_string)
        self.assertEqual(string, 'This Little light of mine ')

    def test_remove_space_before_punctuation(self):
        test_string = 'This , little . Light.. of min4 !'
        string = clean.remove_space_before_punctuation(test_string)
        self.assertEqual(string, 'This, little. Light. of min4!')
    
    def test_remove_stopwords(self):
        test_string = "Tonight, any snow showers in Western MA and far northwest CT's will gradually diminish."
        string = clean.remove_stopwords(test_string)
        self.assertEqual(string, "Tonight, snow showers Western far northwest CT' gradually diminish.")

if __name__ == "__main__":
    unittest.main()
