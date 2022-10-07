import java.io.*;
import java.util.Arrays;
import java.util.PriorityQueue;
import java.util.Random;

public class EPMS  {
    static int INT_NULL = Integer.MAX_VALUE, INT_SIZE = 4, N = 6; // Amount of temp aid files

    static long data_read = 0; // Total amount of read data
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
    static String file_extension = ".bin", working_dir = "src\\";

    public static void main(String[] args) throws IOException {
        File main_file = initFile(working_dir + "main" + file_extension);
        initSort(sortFileByChunks(main_file, (int) Math.min((main_file.length() >> 24), 4096)));
    }

    private static File sortFileByChunks(File target_file, int mb) throws IOException {
        File result_file = new File(target_file.getPath().replaceFirst(file_extension, "") + "_chunks" + file_extension);
        DataInputStream dis = new DataInputStream(new FileInputStream(target_file));
        DataOutputStream dos = new DataOutputStream(new FileOutputStream(result_file));
        byte[] b = new byte[mb << 20];
        int[] int_buff = new int[mb << 18];
        long start = System.currentTimeMillis();
        while (dis.read(b) > 0) {
            for (int i = 0; i < int_buff.length; i+=4)
                int_buff[i >> 2] = b[i] << 24 | (b[i + 1] & 0xFF) << 16 | (b[i + 2] & 0xFF) << 8 | (b[i + 3] & 0xFF);
            Arrays.sort(int_buff);
            dos.write(toByte(int_buff));
        }
        System.out.println("Sort file by chunks of " + mb + "MB phase done successfully in " + (System.currentTimeMillis()-start) + " ms");
        dis.close();
        dos.close();
        return result_file;
    }

    private static byte[] toByte(int[] data) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream(data.length << 2);
        DataOutputStream dos = new DataOutputStream(bos);
        for (int datum : data) dos.writeInt(datum);
        return bos.toByteArray();
    }

    private static void initSort(File main_file) throws IOException {
        DataInputStream[] run_files_dis = new DataInputStream[N + 1];
        File[] working_files = new File[N + 1];
        for (int i = 0; i < working_files.length; i++)
            working_files[i] = new File(working_dir + "aid_temp_" + (i+1) + file_extension);

        distribute(N, working_files, main_file.length(), new DataInputStream(new FileInputStream(main_file)));

        long start = System.currentTimeMillis();
        int min_dummy_values = getMinDummyValue();
        initMergeProcedure(min_dummy_values);
        DataOutputStream dos = new DataOutputStream(new FileOutputStream(working_files[output_file_index]));
        for (int i = 0; i < run_files_dis.length - 1; i++)
            run_files_dis[i] = new DataInputStream(new FileInputStream(working_files[i]));
        while (runs_per_level >= 0) {
            last_elements[output_file_index] = INT_NULL;
            merge(distribution_array[getMinFileIndex()] - min_dummy_values, run_files_dis, dos);

            setPreviousRunDistributionLevel();
            updateOutputFileIndex();
            resetAllowReadArray();

            min_dummy_values = getMinDummyValue();
            dos = new DataOutputStream(new FileOutputStream(working_files[output_file_index]));
            run_files_dis[old_output_file_index] = new DataInputStream(new FileInputStream(working_files[old_output_file_index]));
        }
        dos.close();
        System.out.println("Merge phase done in " + (System.currentTimeMillis() - start) + " ms");
        closeAll(run_files_dis);
        clearTempFiles(main_file, working_files);
    }

    private static void distribute(int temp_files, File[] working_files, long main_file_length, DataInputStream main_file_dis) throws IOException {
        long start = System.currentTimeMillis();
        runs_per_level = 1;
        distribution_array[0] = 1;
        output_file_index = working_files.length - 1;
        int[] write_sentinel = new int[temp_files];
        DataOutputStream[] run_files_dos = new DataOutputStream[temp_files];
        for (int i=0; i < temp_files; i++)
            run_files_dos[i] = new DataOutputStream(new FileOutputStream(working_files[i], false));
        while (data_read < main_file_length) {
            for (int i=0; i < temp_files; i++)
                while (write_sentinel[i] != distribution_array[i]) {
                    while (runsMerged(main_file_length, i, next_run_element))
                        writeNextIntRun(main_file_length, main_file_dis, run_files_dos[i], i);
                    writeNextIntRun(main_file_length, main_file_dis, run_files_dos[i], i);
                    dummy_runs[i]++;
                    write_sentinel[i]++;
                }
            setNextDistributionLevel();
        }
        closeAll(run_files_dos);
        setPreviousRunDistributionLevel();
        setMissingRunsArray();
        System.out.println("Distribute phase done in " + (System.currentTimeMillis() - start) + " ms");
    }

    private static void initMergeProcedure(int min_dummy) {
        for (int i=0; i < dummy_runs.length - 1; i++) dummy_runs[i] -= min_dummy;
        dummy_runs[output_file_index] += min_dummy;
        resetAllowReadArray();
    }

    private static void merge(int min_file_values, DataInputStream[] run_files_dis, DataOutputStream writer) throws IOException {
        int line;
        int min_file;
        int heap_empty = 0;
        IntRecord record;
        populateHeap(run_files_dis);
        while(heap_empty != min_file_values) {
            if ((record = q.poll()) != null) writer.writeInt(record.getValue());
            else return;
            min_file = record.getFileIndex();
            if (allow_read[min_file] && (line = readFileLine(run_files_dis[min_file], min_file)) != INT_NULL)
                q.add(new IntRecord(line, min_file));
            /* Once heap is empty all n-th runs have merged */
            if (q.size() == 0) {
                heap_empty++;
                for (int i = 0; i < next_run_first_elements.length; i++)
                    if (next_run_first_elements[i] != null) {
                        q.add(new IntRecord(next_run_first_elements[i].getValue(), i));
                        last_elements[i] = next_run_first_elements[i].getValue();
                    }
                populateHeap(run_files_dis);
                resetAllowReadArray();
                if (heap_empty == min_file_values) {
                    writer.close();
                    return;
                }
            }
        }
    }

    private static void updateOutputFileIndex() {
        if (output_file_index > 0) output_file_index--;
        else output_file_index = distribution_array.length - 1;
    }

    private static void populateHeap(DataInputStream[] run_files_dis) throws IOException {
        int line;
        for (int i=0; i < run_files_dis.length; i++) {
            if (dummy_runs[i] == 0) {
                if (allow_read[i] && (line = readFileLine(run_files_dis[i], i)) != INT_NULL)
                    q.add(new IntRecord(line, i));
            } else dummy_runs[i]--;
        }
    }

    private static int readFileLine(DataInputStream file_dis, int file_index) throws IOException {
        try {
            int current_int = file_dis.readInt();
            if (last_elements[file_index] != INT_NULL && current_int != INT_NULL && current_int < last_elements[file_index]) {
                next_run_first_elements[file_index] = new IntRecord(current_int, file_index);
                allow_read[file_index] = false;
                return INT_NULL;
            } else last_elements[file_index] = current_int;
            return current_int;
        } catch (EOFException e) {
            return INT_NULL;
        }
    }

    private static void resetAllowReadArray() {
        Arrays.fill(allow_read, true);
        allow_read[output_file_index] = false;
    }

    private static int getMinDummyValue() {
        int min = Integer.MAX_VALUE;
        for (int i = 0; i < dummy_runs.length; i++)
            if (i != output_file_index && dummy_runs[i] < min)
                min = dummy_runs[i];
        return min;
    }

    private static int getMinFileIndex() {
        int min_file_index = 0, min = distribution_array[0];
        for (int i = 1; i < distribution_array.length; i++)
            if (distribution_array[i] != 0 && distribution_array[i] < min)
                min_file_index = i;
        return min_file_index;
    }

    private static void writeNextIntRun(long main_file_length, DataInputStream main_file_dis, DataOutputStream run_file_dos, int file_index) throws IOException {
        try {
            if (data_read >= main_file_length) {dummy_runs[file_index]--; return;}
            if (next_run_element != INT_NULL) {
                run_file_dos.writeInt(next_run_element);
                data_read += INT_SIZE;
            }

            int min_value = Integer.MIN_VALUE;
            int current_int = main_file_dis.readInt();

            /* Case if run is a single element: acordingly update variables and return */
            if (next_run_element != INT_NULL && current_int !=  INT_NULL)
                if (next_run_element > current_int) {
                    run_last_elements[file_index] = next_run_element;
                    next_run_element = current_int;
                    return;
                }

            while(current_int !=  INT_NULL) {
                if (current_int >= min_value) {
                    run_file_dos.writeInt(current_int);
                    data_read += INT_SIZE;
                    min_value = current_int;
                    current_int = main_file_dis.readInt();
                } else {
                    next_run_element = current_int;
                    run_last_elements[file_index] = min_value;
                    return;
                }
            }
        } catch (EOFException e) {}
    }

    private static boolean runsMerged(long main_file_length, int file_index, int first_element) {
        if (data_read < main_file_length && run_last_elements[file_index] != INT_NULL && first_element != INT_NULL)
            return run_last_elements[file_index] <= first_element;
        return false;
    }

    private static void setNextDistributionLevel() {
        runs_per_level = 0;
        int[] current_distribution_array = distribution_array.clone();
        for (int i = 0; i < current_distribution_array.length - 1; i++) {
            distribution_array[i] = current_distribution_array[0] + current_distribution_array[i+1];
            runs_per_level +=  distribution_array[i];
        }
    }

    private static void setPreviousRunDistributionLevel() {
        int diff;
        int[] current_distribution_array = distribution_array.clone();
        int last = current_distribution_array[current_distribution_array.length - 2];

        old_output_file_index = output_file_index;

        runs_per_level = 0;
        runs_per_level += last;
        distribution_array[0] = last;

        for (int i = current_distribution_array.length - 3; i >= 0; i--) {
            diff = current_distribution_array[i] - last;
            distribution_array[i+1] = diff;
            runs_per_level += diff;
        }
    }

    private static void setMissingRunsArray() {
        for (int i = 0; i < distribution_array.length - 1; i++)
            dummy_runs[i] = distribution_array[i] - dummy_runs[i];
    }

    private static void clearTempFiles(File main_file, File[] temp_files) throws IOException {
        for (File temp_file : temp_files) {
            System.out.println("MAIN LEN: " + main_file.length() + " - TEMP LEN: " + temp_file.length());
            if (temp_file.length() != 0) {
                temp_file.renameTo(new File(working_dir + "sorted" + file_extension));
                System.out.println("Is file " + temp_file.getName() + " sorted? - " + isFileSorted(temp_file));
            } else {
                temp_file.delete();
                System.out.println(" - Delete temp file: " + temp_file.getName());
            }
        }
    }

    private static boolean isFileSorted(File temp_file) throws IOException {
        DataInputStream dis = new DataInputStream(new FileInputStream(temp_file));
        try {
            int first = dis.readInt(); //4 bytes
            dis.skip(temp_file.length() - 2 << 2); // 4 first and 4 last bytes reserve for last int
            int last = dis.readInt();
            if (first > last) return false;
        } catch (EOFException e) {}
        dis.close();
        return true;
    }

    private static void closeAll(Closeable[] c) throws IOException {for (Closeable o: c) o.close();}

    public static File initFile(String path) throws IOException {
        System.out.print("Бажаєте скористатися якимось файлом або згенерувати новий (0/1)? ");
        BufferedReader console = new BufferedReader(new InputStreamReader(System.in));
        File data;
        if (Integer.parseInt(console.readLine()) == 1) {
            System.out.print("На скільки МБ ви бажаєте згенерувати чисел цілого типу? ");
            int mb = Integer.parseInt(console.readLine());
            DataOutputStream dos = new DataOutputStream(new FileOutputStream(data = new File(path)));
            Random random = new Random();
            System.out.println("Процес генерації нового файлу...");
            long start = System.currentTimeMillis();
            int[] tmp = new int[mb << 18];
            for (int i = 0; i < mb << 18; i++)
                tmp[i] = random.nextInt(INT_NULL); // 18 = 20 (mb) - 2 (int)
            dos.write(toByte(tmp));
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