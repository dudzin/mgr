package pl.pw.elka.distmatrix;

import java.util.ArrayList;

public class ForbWords {

	private ArrayList<String> words;
	
	public ForbWords() {
		words = new ArrayList<String>();
	}
	
	public boolean isWorb(String word){
		for (String w : words) {
			if(w.equals(word)) return true;
		}
		return false;
	}
	

	public ArrayList<String> getWords() {
		return words;
	}

	public void setWords(ArrayList<String> words) {
		this.words = words;
	}
	
	public void addWord(String word){
		words.add(word);
	}
}
