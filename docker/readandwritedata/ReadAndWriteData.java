import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.List;

public class ReadAndWriteData {
    public static void main(String[] args) {
        String filePath = "/data/numbers.txt";
        int lastNumber = readLastNumber(filePath);
        System.out.println(lastNumber);
        for (int i = 0; i<10000; ++i){
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            lastNumber = addNumberAndWriteInFile(lastNumber,2,filePath);
        }
    }

    private static int addNumberAndWriteInFile(int lastNumber, int i, String filePath) {
        int newNumber = lastNumber+2;
        try{
            Files.writeString(Paths.get(filePath), newNumber+"\n", StandardOpenOption.APPEND);

        } catch (IOException e) {
            e.printStackTrace();
        }
        return newNumber;
    }

    private static int readLastNumber(String filePath) {
        try {
            List<String> allLines = Files.readAllLines(Paths.get(filePath));
            return (int) Integer.parseInt(allLines.get(allLines.size()-1));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return 0;
    }


    
}
