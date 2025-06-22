package hr.fipu.raft.utils;

public class Entry {
    private final int term;
    private final String command;

    public Entry(int term, String command) {
        this.term = term;
        this.command = command;
    }

    public int getTerm() {
        return this.term;
    }

    public String getCommand() {
        return this.command;
    }

    @Override
    public String toString() {
        return "Entry{" +
                "term=" + this.term +
                ", command='" + this.command + '\'' +
                '}';
    }
}
