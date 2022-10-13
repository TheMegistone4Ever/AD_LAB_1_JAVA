import java.io.*;
import java.util.Arrays;
import java.util.PriorityQueue;
import java.util.Random;

public class EPMS  {
    public record IntRecord(int value, int file_index) implements Comparable<IntRecord> {
        public int getValue() {return value;}
        public int getFileIndex() {return file_index;}
        @Override public int compareTo(IntRecord o) {return Integer.compare(this.value, o.value);}
    }

    static int INT_NULL = Integer.MAX_VALUE, INT_SIZE = 4, N = 6; // Amount of temp aid files

    static long data_read; // Total amount of read data
    static int next_run_element; // First element of next run
    static int output_file_index; // Index of current active output file where runs are being merged
    static int old_output_file_index; // Index of previous active output file (previous distribution level)
    static int runs_per_level; // Amount of runs on current distribution level

    static int[] dummy_runs = new int[N + 1]; // Array used to store dummy runs for each input file after distribute phase
    static int[] distribution_array = new int[N + 1]; // Array used to determine distribution of runs in input files
    static boolean[] allow_read = new boolean[N + 1]; // Array used as a marker for input file readers
    static int[] last_elements = new int[N + 1]; // All last elements of the current runs from each input file
    static int[] run_last_elements = new int[N + 1]; // Used to store all last elements of the current runs from each input file
    static IntRecord[] next_run_first_elements = new IntRecord[N + 1]; // used to store all first elements of the next runs from each input file
    static PriorityQueue<IntRecord> q = new PriorityQueue<>(); // Used to extract next minimum int that needs to be written to output file.
    static byte[] writeBuffer = new byte[4];

    static long mainFileLength;

    public static void main(String[] args) throws IOException {
        System.out.print("Ви бажаєте з опитимізацією чи ні? ");
        boolean isOptimized = Integer.parseInt(new BufferedReader(new InputStreamReader(System.in)).readLine()) > 0;
        File file = initFile( "main.bin");
        mainFileLength = file.length();
        ExternalPolyphaseMergesort(isOptimized ? sortFileByChunks(file, (int)Math.min(file.length() >> 3, 1 << 24)) : file);
    }

    private static File sortFileByChunks(File target_file, int bLen) throws IOException {
        File result_file = new File(target_file.getPath().replaceFirst(".bin", "") + "_chunks.bin");
        DataInputStream dis = new DataInputStream(new FileInputStream(target_file));
        DataOutputStream dos = new DataOutputStream(new FileOutputStream(result_file));
        byte[] b = new byte[bLen];
        int[] int_buff = new int[bLen >> 2];
        long start = System.currentTimeMillis();
        while (dis.read(b) >= INT_SIZE) {
            for (int i = 0; i < int_buff.length; i += INT_SIZE)
                int_buff[i >> 2] = b[i] << 24 | (b[i + 1] & 0xFF) << 16 | (b[i + 2] & 0xFF) << 8 | (b[i + 3] & 0xFF);
            Arrays.sort(int_buff);
            dos.write(toByte(int_buff));
        }
        System.out.println("Sort file by chunks of " + bLen + "B phase done successfully in " + (System.currentTimeMillis()-start) + " ms");
        return result_file;
    }

    private static byte[] toByte(int[] d) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream(d.length << 2);
        DataOutputStream dos = new DataOutputStream(bos);
        for (int i : d) dos.writeInt(i);
        return bos.toByteArray();
    }

    private static void ExternalPolyphaseMergesort(File main_file) throws IOException {
        DataInputStream[] run_files_dis = new DataInputStream[N + 1];
        File[] working_files = new File[N + 1];
        for (int i = 0; i < working_files.length; i++)
            working_files[i] = new File("aid_temp_" + (i+1) + ".bin");
        distribute(N, working_files, new DataInputStream(new FileInputStream(main_file)));
        long start = System.currentTimeMillis();
            int min_dummy_values = getMinDummyValue();
            initMergeProcedure(min_dummy_values);
            DataOutputStream dos = new DataOutputStream(new FileOutputStream(working_files[output_file_index]));
            for (int i = 0; i < run_files_dis.length - 1; i++)
                run_files_dis[i] = new DataInputStream(new FileInputStream(working_files[i]));
            while (runs_per_level > 0) {
                last_elements[output_file_index] = INT_NULL;
                merge(distribution_array[getMinFileIndex()] - min_dummy_values, run_files_dis, dos);
                setPreviousRunDistributionLevel();
                output_file_index = (output_file_index > 0 ? output_file_index : distribution_array.length) - 1;
                resetAllowReadArray();
                min_dummy_values = getMinDummyValue();
                dos = new DataOutputStream(new FileOutputStream(working_files[output_file_index]));
                run_files_dis[old_output_file_index] = new DataInputStream(new FileInputStream(working_files[old_output_file_index]));
            }
        System.out.println("Merge phase done in " + (System.currentTimeMillis() - start) + " ms");
        dos.close();
        closeAll(run_files_dis);
        outAidFiles(main_file, working_files);
    }

    private static void distribute(int temp_files, File[] working_files, DataInputStream main_file_dis) throws IOException {
        long start = System.currentTimeMillis();
        runs_per_level = 1;
        distribution_array[0] = 1;
        output_file_index = working_files.length - 1;
        int[] write_sentinel = new int[temp_files];
        DataOutputStream[] run_files_dos = new DataOutputStream[temp_files];
        for (int i=0; i < temp_files; i++)
            run_files_dos[i] = new DataOutputStream(new FileOutputStream(working_files[i]));
        while (data_read < mainFileLength) {
            for (int i=0; i < temp_files; i++)
                while (write_sentinel[i] != distribution_array[i]) {
                    while (data_read < mainFileLength && next_run_element != INT_NULL && run_last_elements[i] <= next_run_element)
                        writeNextIntRun(main_file_dis, run_files_dos[i], i);
                    writeNextIntRun(main_file_dis, run_files_dos[i], i);
                    dummy_runs[i]++;
                    write_sentinel[i]++;
                }
            setNextDistributionLevel();
        }
        setPreviousRunDistributionLevel();

        // set missing runs array
        for (int i = 0; i < distribution_array.length - 1; i++)
            dummy_runs[i] = distribution_array[i] - dummy_runs[i];

        System.out.println("Distribute phase done in " + (System.currentTimeMillis() - start) + " ms");
    }

    private static void initMergeProcedure(int min_dummy) {
        for (int i=0; i < dummy_runs.length - 1; i++) dummy_runs[i] -= min_dummy;
        dummy_runs[output_file_index] += min_dummy;
        resetAllowReadArray();
    }

    private static void merge(int min_file_values, DataInputStream[] run_files_dis, DataOutputStream writer) throws IOException {
        int num, min_file, heap_empty = 0;
        IntRecord record;
        populateHeap(run_files_dis);
        while (heap_empty != min_file_values) {
            if ((record = q.poll()) == null) return;
            writer.writeInt(record.getValue());
            min_file = record.getFileIndex();
            if (allow_read[min_file])
                if ((num = readInt(run_files_dis[min_file], min_file)) != INT_NULL)
                    q.add(new IntRecord(num, min_file));
            if (q.size() == 0) {
                heap_empty++;
                for (int i = 0; i < next_run_first_elements.length; i++) {
                    if (next_run_first_elements[i] == null) break;
                    q.add(new IntRecord(next_run_first_elements[i].getValue(), i));
                    last_elements[i] = next_run_first_elements[i].getValue();
                }
                populateHeap(run_files_dis);
                resetAllowReadArray();
            }
        }
    }

    private static void populateHeap(DataInputStream[] run_files_dis) throws IOException {
        for (int i = 0; i < run_files_dis.length; i++)
            if (dummy_runs[i] == 0) {
                if (allow_read[i]) q.add(new IntRecord(readInt(run_files_dis[i], i), i));
            } else dummy_runs[i]--;
    }

    private static int readInt(DataInputStream file_dis, int file_index) throws IOException {
        if (file_dis.read(writeBuffer) < INT_SIZE) return INT_NULL;
        int current_int = buffToInt();
        if (last_elements[file_index] != INT_NULL)
            if (current_int < last_elements[file_index]) {
                next_run_first_elements[file_index] = new IntRecord(current_int, file_index);
                allow_read[file_index] = false;
                return INT_NULL;
            }
        return last_elements[file_index] = current_int;
    }

    private static void resetAllowReadArray() {
        Arrays.fill(allow_read, true);
        allow_read[output_file_index] = false;
    }

    private static int getMinDummyValue() {
        int min = dummy_runs[0];
        for (int i = 1; i < dummy_runs.length; i++)
            if  (dummy_runs[i] < min)
                if (i != output_file_index) min = dummy_runs[i];
        return min;
    }

    private static int getMinFileIndex() {
        int min_file_index = 0, min = distribution_array[0];
        for (int i = 1; i < distribution_array.length; i++)
            if (distribution_array[i] != 0)
                if (distribution_array[i] < min) min_file_index = i;
        return min_file_index;
    }

    private static void writeNextIntRun(DataInputStream main_file_dis, DataOutputStream run_file_dos, int file_index) throws IOException {
        if (data_read >= mainFileLength) {dummy_runs[file_index]--; return;}
        if (next_run_element != INT_NULL) {
            run_file_dos.writeInt(next_run_element);
            data_read += INT_SIZE;
        }

        int min_value = Integer.MIN_VALUE;
        if (main_file_dis.read(writeBuffer) < INT_SIZE) return;

        int current_int = buffToInt();

        while(current_int !=  INT_NULL) {
            if (current_int >= min_value) {
                run_file_dos.writeInt(current_int);
                data_read += INT_SIZE;
                min_value = current_int;
                if (main_file_dis.read(writeBuffer) < INT_SIZE) break;
                current_int = buffToInt();
            } else {
                next_run_element = current_int;
                run_last_elements[file_index] = min_value;
                break;
            }
        }
    }

    private static int buffToInt() {
        return writeBuffer[0] << 24 | (writeBuffer[1] & 0xFF) << 16 | (writeBuffer[2] & 0xFF) << 8 | (writeBuffer[3] & 0xFF);
    }

    private static void setNextDistributionLevel() {
        runs_per_level = 0;
        int[] clone = distribution_array.clone();
        for (int i = 0; i < clone.length - 1; runs_per_level += distribution_array[++i])
            distribution_array[i] = clone[0] + clone[i+1];
    }

    private static void setPreviousRunDistributionLevel() {
        int[] clone = distribution_array.clone();
        old_output_file_index = output_file_index;
        distribution_array[0] = runs_per_level = clone[clone.length - 2];
        for (int diff, i = clone.length - 3; i >= 0; i--, runs_per_level += diff)
            distribution_array[i+1] = diff = clone[i] - clone[clone.length - 2];
    }

    private static void outAidFiles(File main_file, File[] temp_files) throws IOException {
        for (File temp_file : temp_files) {
            System.out.print("MAIN LEN: " + main_file.length() + " - TEMP LEN: " + temp_file.length());
            System.out.println(temp_file.length() != 0 ? " - Is file " + temp_file.getName() + " sorted? - " + isFileSorted(temp_file) : "");
        }
    }

    private static boolean isFileSorted(File temp_file) throws IOException {
        DataInputStream dis = new DataInputStream(new FileInputStream(temp_file));
        if (dis.read(writeBuffer) < INT_SIZE) return false;
        int first = buffToInt();
        if (dis.skip(temp_file.length() - 8) <= 0) return false;
        if (dis.read(writeBuffer) < INT_SIZE) return false;
        dis.close();
        return first <= buffToInt();
    }

    private static void closeAll(Closeable[] c) throws IOException {for (Closeable o: c) o.close();}

    public static File initFile(String path) throws IOException {
        System.out.print("Бажаєте скористатися якимось файлом або згенерувати новий (0/1)? ");
        BufferedReader console = new BufferedReader(new InputStreamReader(System.in));
        File data;
        if (Integer.parseInt(console.readLine()) > 0) {
            System.out.print("На скільки МБ ви бажаєте згенерувати чисел цілого типу? ");
            int mb = Integer.parseInt(console.readLine());
            DataOutputStream dos = new DataOutputStream(new FileOutputStream(data = new File(path)));
            Random random = new Random();
            System.out.println("Процес генерації нового файлу...");
            long start = System.currentTimeMillis();
            if (mb <= 1 << 8) {
                int[] tmp = new int[mb << 18];
                for (int i = 0; i < mb << 18; i++) tmp[i] = random.nextInt(INT_NULL);
                dos.write(toByte(tmp));
            } else {
                int[] tmp = new int[1 << 8];
                for (int i = 0; i < mb << 10; i++) {
                    for (int j = 0; j < 1 << 8; j++) tmp[j] = random.nextInt(INT_NULL);
                    dos.write(toByte(tmp));
                }
            }
            System.out.println("Файл розміром " + mb + " МБ успішно згенеровано за " + (System.currentTimeMillis()-start) + " ms");
            dos.close();
        } else {
            System.out.print("Вкажіть шлях до файлу: ");
            data = new File(console.readLine());
        }
        console.close();
        return data;
    }
}