package services;

/**
 * Created by Владимир on 28.10.2016.
 */
public enum Task {
    SCHOOL("schools"),
    UNIVERSITY("universities"),
    MILITARY("military"),
    CAREER("career");

    private String name;

    Task(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }
}
