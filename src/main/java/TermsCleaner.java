import java.util.Arrays;
import java.util.List;
import java.util.ListIterator;

public class TermsCleaner {

    public List<String> clean(List<String> rawTerms){
      removeStopWords(rawTerms);
      removeAllCharacterInstances("-", rawTerms);
      removeWordsShorterThan(2, rawTerms);
      return rawTerms;
    }
    private void removeStopWords(List<String> wordList){
        wordList.removeAll(stopWords());
    }
    private void removeWordsShorterThan(int length, List<String> wordList){
        ListIterator<String> titleTermsIterator = wordList.listIterator();

        while (titleTermsIterator.hasNext()){
            String currentWord = titleTermsIterator.next();
            int currentWordIndex = titleTermsIterator.nextIndex();
            if (currentWord.length() < length){
                wordList.remove(currentWordIndex);
            }
        }
    }
    private void removeAllCharacterInstances(String targetCharacter, List<String> wordList){
        for (String word: wordList){
          word.replaceAll(targetCharacter, "");
        }
    }
    private List<String> stopWords(){
         return Arrays.asList("about", "above", "after", "again", "against", "all", "am", "an", "and", "any",
                "are", "arent", "as", "at", "be", "because", "been", "before", "being", "below", "between", "both", "but",
                "by", "cant", "cannot", "could", "couldnt", "did", "didnt", "do", "does", "doesnt", "doing", "dont", "down",
                "during", "each", "few", "for", "from", "further", "had", "hadnt", "has", "hasnt", "have", "havent", "having",
                "he", "hed", "hell", "hes", "her", "here", "heres", "hers", "herself", "him", "himself", "his", "how", "hows",
                "i", "id", "ill", "im", "ive", "if", "in", "into", "is", "isnt", "it", "its", "its", "itself", "lets", "me",
                "more", "most", "mustnt", "my", "myself", "no", "nor", "not", "of", "off", "on", "once", "only", "or", "other",
                "ought", "our", "ours ", "ourselves", "out", "over", "own", "same", "shant", "she", "shed", "shell", "shes",
                "should", "shouldnt", "so", "some", "such", "than", "that", "thats", "the", "their", "theirs", "them",
                "themselves", "then", "there", "theres", "these", "they", "theyd", "theyll", "theyre", "theyve", "this",
                "those", "through", "to", "too", "under", "until", "up", "very", "was", "wasnt", "we", "wed", "well", "were",
                "weve", "were", "werent", "what", "whats", "when", "whens", "where", "wheres", "which", "while", "who", "whos",
                "whom", "why", "whys", "with", "wont", "would", "wouldnt", "you", "youd", "youll", "youre", "youve", "your",
                "yours", "yourself", "yourselves");
    }

}
