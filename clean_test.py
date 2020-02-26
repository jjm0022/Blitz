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


if __name__ == "__main__":
    unittest.main()