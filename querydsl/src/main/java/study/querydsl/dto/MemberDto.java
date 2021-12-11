package study.querydsl.dto;

import com.querydsl.core.annotations.QueryProjection;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class MemberDto {

    private String username;
    private int age;

    @QueryProjection // dto를 q파일로 생성하게 만들어줌
    public MemberDto(String username, int age) {
        this.username = username;
        this.age = age;
    }
}
