/**
 * Created by Владимир on 26.10.2016.
 */
public enum Relations {
    FRIEND(0),
    SCHOOL_FRIEND(26),
    SCHOOL_BESTFRIEND(37),
    UNIVERSITY_FRIEND(25),
    UNIVERSITY_BESTFRIEND(38),
    MILITARY_FRIEND(36);

    Relations(Integer code) {
        this.code = code;
    }

    private Integer code;

    public Integer getCode() {
        return code;
    }
}
