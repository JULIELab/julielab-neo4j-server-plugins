package de.julielab.neo4j.plugins;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.neo4j.shell.util.json.JSONException;
import org.neo4j.unsafe.batchinsert.BatchInserter;

import com.google.gson.Gson;

import de.julielab.neo4j.plugins.ahocorasick.ACFactoryBatch;
import de.julielab.neo4j.plugins.ahocorasick.property.ACDataBase;
import de.julielab.neo4j.plugins.ahocorasick.property.ACDictionary;
import de.julielab.neo4j.plugins.ahocorasick.property.ACEntry;
import de.julielab.neo4j.plugins.test.TestUtilities;

public class ACBenchmarking {

	/**
	 * @param args
	 * @throws IOException 
	 * @throws JSONException 
	 */
	public static void main(String[] args) throws IOException, JSONException {
			
		/***** EINLESEN DER DATEN ****/
		Charset charset = StandardCharsets.UTF_8;
		File fIn = new File("src/test/resources/namelist/authornamesTestSet1Mio");
		File fOut = new File("src/test/resources/benchmark");
		
		if(!fOut.exists()){
			fOut.createNewFile();
		}
		
		BufferedWriter bw = new BufferedWriter(new FileWriter(fOut));
		
		LineIterator it;
		int runs = 1;
		Gson g = new Gson();
		
		final String dictNameTree = "NamenBaum";
		ACDictionary dict = new ACDictionary(dictNameTree,+
				ACDictionary.GLOBAL_MODE_CREATE, 
				ACDictionary.LOCAL_MODE_SEARCH);

		// Benchmarking bauen
		for(int j = 500000 ; j<=500000; j+=500000){
			System.out.println("Bau :"+j);
			long timeCompleteBuild =  0;
			long timeDictBuild = 0;
			long timeAddPrepare = 0;
			long timeUnprepare = 0;
			long timeDelete = 0;
			
			ACDataBase db = new ACDataBase(TestUtilities.GRAPH_DB_DIR);
			BatchInserter batchInserter = db.startBatchInserter();
//			GraphDatabaseService graphDb = db.startGraphDatabase();
			
			for(int i = 0; i<1; i++){
				
				ACFactoryBatch.createDictTree(batchInserter, dict);
//				ACFactoryEmbedded.createDictTree(graphDb, dictNameTree+j);
				it = FileUtils.lineIterator(fIn, charset.name());
				int counter = 0;

				while(it.hasNext() && counter < j){
					// Einträge komplett
					List<ACEntry> entriesComplete = new ArrayList<>();
					
					int a = 0;
					while (it.hasNext() && counter < j && a < 10000) {
						
						String next = it.next();
						ACEntry entry = new ACEntry(next, next.split(" "));
						entry.addAttribute("attribute1", "attValue1");
						
						entriesComplete.add(entry);
						
						counter++;
						a++;
					}
					
					// Liste vorbereiten für das Übertragen
					String json = g.toJson(entriesComplete);
					
					// Vorher aus Index löschen
					
					System.out.println("Start: "+counter);
					
					long timeStart = System.currentTimeMillis();
					ACFactoryBatch.addListToDictTree(db, dict, entriesComplete);
//					ACFactoryEmbedded.addListToDictTree(graphDb, dictNameTree+j, json);
					timeDictBuild += System.currentTimeMillis()-timeStart;
					
					System.out.println("End: "+counter);
				}
				
				
				if(db.stopBatchInserter()){
					batchInserter = db.startBatchInserter();
					long timeStart = System.currentTimeMillis();
					ACFactoryBatch.prepareDictTreeForSearch(db, dict);
//					ACFactoryEmbedded.prepareDictTreeForSearch(graphDb, dictNameTree+j);
					timeCompleteBuild += System.currentTimeMillis()-timeStart;
				}
				
				
				db.stopBatchInserter();

				counter = 0;
				
//				timeStart = System.currentTimeMillis();
//				ahoCorasick.unprepareDictTree(graphDb, dictNameTree);
//				timeUnprepare += System.currentTimeMillis() - timeStart;
//				
//				timeStart = System.currentTimeMillis();
//				ahoCorasick.deleteDictTree(graphDb, dictNameTree);
//				timeDelete += System.currentTimeMillis() - timeStart;
				
				bw.write("Anzahl der Einträge "+j);
				bw.newLine();
				bw.write("Baum: "+TimeUnit.MILLISECONDS.toSeconds(timeDictBuild/runs)+" sec");
				bw.newLine();
				bw.write("Prepare: "+TimeUnit.MILLISECONDS.toSeconds(timeCompleteBuild/runs)+" sec");
				bw.newLine();
				bw.write("Add100k: "+TimeUnit.MILLISECONDS.toSeconds(timeAddPrepare/runs)+" sec");
				bw.newLine();
				bw.write("Fail-Relationen Löschen: "+TimeUnit.MILLISECONDS.toSeconds(timeUnprepare/runs)+" sec");
				bw.newLine();
				bw.write("Delete: "+TimeUnit.MILLISECONDS.toSeconds(timeDelete/runs)+" sec");
				bw.newLine();
				bw.flush();
				
			}
			
//			dict.changeMode(ACDictionary.LOCAL_MODE);
//			ACDictionary dict2 = new ACDictionary(dict.name(), ACDictionary.GLOBAL_MODE, false);
//			GraphDatabaseService graphDb = db.startGraphDatabase();
			
//			 Benchmarking bauen
//			Random rand = new Random();
//			for(int k = 1000 ; k<=10000; k+=1000){
//				long searchTime = 0;
//				for(int i = 0; i<runs; i++){
//					it = FileUtils.lineIterator(fIn, charset.name());
//					
//					int counter = 0;
//					while (it.hasNext() && counter < k) {
//						
//						String entry = it.next();
//						
//						int begin = rand.nextInt(10);
//						int end = rand.nextInt(10);
//						
//						// Suchbegriff erstellen, mit zufälligen Buchstaben davor und danach
//						StringBuffer search = new StringBuffer();
//						
//						for(int h = 0; h<begin ; h++){
//							search.append(TestUtilities.randomLetter());
//						}
//						
//						search.append(entry);
//						
//						for(int h = 0; h<end ; h++){
//							search.append(TestUtilities.randomLetter());
//						}
//						
//						// Anfrage
//						long timeStart = System.currentTimeMillis();
//						ACSearch.completeSearch(graphDb, g.toJson(dict2), search.toString());	
//						searchTime += System.currentTimeMillis() - timeStart;
//						counter++;
//					}
//				}
//				
//				System.out.println(k);
//				
//				bw.write("Anzahl der Suchbegriffe "+k);
//				bw.newLine();
//				bw.write("Zeit: "+TimeUnit.MILLISECONDS.toSeconds(searchTime/runs)+" sec");
//				bw.newLine();
//				bw.newLine();
//				bw.flush();
//			}
//			
//			db.stopGraphDatabase();
//			
			bw.close();
		}
	}
	
	
	public static void deleteFolder(File folder) {
	    File[] files = folder.listFiles();
	    if(files!=null) { //some JVMs return null for empty dirs
	        for(File f: files) {
	            if(f.isDirectory()) {
	                deleteFolder(f);
	            } else {
	                f.delete();
	            }
	        }
	    }
	    folder.delete();
	}

}
