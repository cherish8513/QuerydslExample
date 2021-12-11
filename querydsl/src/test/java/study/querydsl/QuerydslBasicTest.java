package study.querydsl;

import com.querydsl.core.BooleanBuilder;
import com.querydsl.core.Tuple;
import com.querydsl.core.types.ExpressionUtils;
import com.querydsl.core.types.Predicate;
import com.querydsl.core.types.Projections;
import com.querydsl.core.types.dsl.BooleanExpression;
import com.querydsl.core.types.dsl.Expressions;
import com.querydsl.jpa.JPAExpressions;
import com.querydsl.jpa.impl.JPAQueryFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.transaction.annotation.Transactional;
import study.querydsl.domain.Member;
import study.querydsl.domain.QMember;
import study.querydsl.domain.QTeam;
import study.querydsl.domain.Team;
import study.querydsl.dto.MemberDto;
import study.querydsl.dto.QMemberDto;

import javax.persistence.EntityManager;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static study.querydsl.domain.QMember.*;
import static study.querydsl.domain.QTeam.*;

@SpringBootTest
@Transactional
public class QuerydslBasicTest {

    @Autowired
    private EntityManager em;

    private JPAQueryFactory query;
    // 스태틱 QMember가 별칭 member라는 이름으로 생성됐는데
    // member라는 별칭을 사용하면 컴파일시 인식을 못하는 오류를 발생해서 새로 생성한다.
    private final QMember member = new QMember("member1");

    @BeforeEach
    public void before() throws Exception{
        query = new JPAQueryFactory(em);
        Team teamA = new Team("teamA");
        Team teamB = new Team("teamB");
        em.persist(teamA);
        em.persist(teamB);

        Member member1 = new Member("member1", 10, teamA);
        Member member2 = new Member("member2", 20, teamA);
        Member member3 = new Member("member3", 30, teamB);
        Member member4 = new Member("member4", 40, teamB);
        em.persist(member1);
        em.persist(member2);
        em.persist(member3);
        em.persist(member4);

        em.flush();
        em.clear();
    }

    @Test
    public void startJPQL() throws Exception{
        //given

        //when
        Member findMember = em.createQuery("select m from Member m where m.username = :username", Member.class)
                .setParameter("username", "member1")
                .getSingleResult();

        //then
        assertEquals(findMember.getUsername(), "member1");
    }

    @Test
    public void startQuerydsl() throws Exception{
        //when
        Member findMember = query
                .select(member)
                .from(member)
                .where(member.username.eq("member1"))
                .fetchOne();

        //then
        assertEquals(findMember.getUsername(), "member1");
    }

    @Test
    public void search() throws Exception{
        //when
        Member findMember = query.selectFrom(member)
                .where(member.username.eq("member1")
                        .and(member.age.eq(10)))
                .fetchOne();

        //then
        assertEquals(findMember.getUsername(), "member1");
        assertEquals(findMember.getAge(), 10);
    }

    @Test
    public void searchAndParam() throws Exception{
        //when
        Member findMember = query.selectFrom(member)
                .where(member.username.eq("member1") , (member.age.eq(10)), null)
                .fetchOne();

        //then
        assertEquals(findMember.getUsername(), "member1");
        assertEquals(findMember.getAge(), 10);
    }

    @Test
    public void sort() throws Exception{
        //given
        em.persist(new Member(null, 100, null));
        em.persist(new Member("member5", 100, null));
        em.persist(new Member("member6", 100, null));

        //when
        List<Member> result = query
                .selectFrom(member)
                .where(member.age.eq(100))
                .orderBy(member.age.desc(), member.username.asc().nullsLast())
                .fetch();

        //then
        Member member5 = result.get(0); // 나이는 모두 같고 이름 오름차순 null은 맨 뒤
        Member member6 = result.get(1);
        Member memberNull = result.get(2);
        assertEquals(member5.getUsername(), "member5");
        assertEquals(member6.getUsername(), "member6");
        assertEquals(memberNull.getUsername(), null);
    }

    @Test
    public void paging1() throws Exception{
        //when
        List<Member> result = query.selectFrom(member) // 페이지 쿼리가 복잡하면 count를 따로 작성
                .orderBy(member.username.desc())
                .offset(1)
                .limit(2)
                .fetch();

        //then
        assertEquals(result.size(), 2);
    }

    @Test
    public void aggregation() throws Exception{
        //given
        List<Tuple> result = query
                .select(
                        member.count(),
                        member.age.sum(),
                        member.age.avg(),
                        member.age.max(),
                        member.age.min())
                .from(member)
                .fetch();
        //when
        Tuple tuple = result.get(0); // 튜플은 잘 안쓴다 나중에 DTO로 변경해서 사용

        //then
        assertEquals(tuple.get(member.count()), 4);
        assertEquals(tuple.get(member.age.sum()), 100);
        assertEquals(tuple.get(member.age.avg()), 25);
        assertEquals(tuple.get(member.age.max()), 40);
        assertEquals(tuple.get(member.age.min()), 10);
    }

    @Test
    public void group() throws Exception{
        //given
        List<Tuple> result = query
                .select(team.name, member.age.avg()) // QMember는 default가 오류 발생시키는데 QTeam은 괜찮네..
                .from(member)
                .join(member.team, team)
                .groupBy(team.name)
                .fetch();

        //when
        Tuple teamA = result.get(0); // 그룹이 팀 A와 팀 B로 나뉜다.
        Tuple teamB = result.get(1);

        //then
        assertEquals(teamA.get(team.name), "teamA");
        assertEquals(teamA.get(member.age.avg()), 15);
        assertEquals(teamB.get(team.name), "teamB");
        assertEquals(teamB.get(member.age.avg()), 35);
    }

    @Test
    public void join() throws Exception{
        //given
        List<Member> result = query
                .selectFrom(member)
                .join(member.team, team)
                .where(team.name.eq("teamA"))
                .fetch();

        //when

        //then
        assertEquals(result.get(0).getUsername(), "member1");
        assertEquals(result.get(1).getUsername(), "member2");
    }

    @Test
    public void join2() throws Exception{
        //given
        List<Member> result = query // jpql에선 연관관계가 있지만 sql엔 없어서 세타 조인으로 카티션 곱을 적용한다
                .selectFrom(member)
                .where(member.team.name.eq("teamA"))
                .fetch();

        //when

        //then
        assertEquals(result.get(0).getUsername(), "member1");
        assertEquals(result.get(1).getUsername(), "member2");
    }

    @Test
    public void join_on_filtering() throws Exception{
        //when
        List<Tuple> result = query
                .select(member, team)
                .from(member)
                .leftJoin(member.team, team).on(team.name.eq("teamA"))
                .fetch();

        //then
        for (Tuple tuple: result) {
            System.out.println(tuple);
        }
    }

    @Test
    public void patchJoinNo() throws Exception{
        //given
        em.flush();
        em.clear();

        //when
        Member findMember = query
                .selectFrom(member)
//                .join(member.team, team).fetchJoin()
                .where(member.username.eq("member1"))
                .fetchOne();

        //then
        System.out.println(findMember.getTeam().getName()); // 패치조인 안 했으면 select 쿼리 2번 나갔을 것
    }

    @Test
    public void patchJoinUse() throws Exception{
        //given
        em.flush();
        em.clear();

        //when
        Member findMember = query
                .selectFrom(member)
                .join(member.team, team).fetchJoin()
                .where(member.username.eq("member1"))
                .fetchOne();

        //then
        System.out.println(findMember.getTeam().getName());
    }

    // subQuery는 from 절에서는 사용할 수 없다.
    @Test
    public void subQuery() throws Exception{
        //given
        QMember memberSub = new QMember("memberSub");
        List<Member> result = query
                .selectFrom(member)
                .where(member.age.eq(
                        JPAExpressions
                                .select(memberSub.age.max())
                                .from(memberSub)
                ))
                .fetch();
        //when
        Member m = result.get(0);

        //then
        assertEquals(m.getAge(), 40);
    }

    @Test
    public void subQueryGoe() throws Exception{
        //given
        QMember memberSub = new QMember("memberSub");
        List<Member> result = query
                .selectFrom(member)
                .where(member.age.goe(
                        JPAExpressions
                                .select(memberSub.age.avg())
                                .from(memberSub)
                ))
                .fetch();
        //when

        //then
        for (Member m : result) {
            assertTrue(m.getAge() >= 20);
        }
    }

    @Test
    public void findDtoBySetter() throws Exception{
        //when
        List<MemberDto> result = query
                .select(Projections.bean(MemberDto.class, member.username, member.age))
                .from(member)
                .fetch();

        //then
        for (MemberDto memberDto : result) {
            System.out.println(memberDto.getUsername());
            System.out.println(memberDto.getAge());
        }
    }

    @Test
    public void findDtoByFields() throws Exception{
        QMember memberSub = new QMember("memberSub");
        //when
        List<MemberDto> result = query
                .select(Projections.fields(MemberDto.class, this.member.username,
                        ExpressionUtils.as(JPAExpressions
                                .select(memberSub.age.max())
                                .from(memberSub), "age")
                ))
                .from(this.member)
                .fetch();

        //then
        for (MemberDto memberDto : result) {
            System.out.println(memberDto.getUsername());
            System.out.println(memberDto.getAge());
        }
    }

    @Test
    public void findDtoByConstructor() throws Exception{
        //when
        List<MemberDto> result = query
                .select(Projections.constructor(MemberDto.class, member.username, member.age))
                .from(member)
                .fetch();

        //then
        for (MemberDto memberDto : result) {
            System.out.println(memberDto.getUsername());
            System.out.println(memberDto.getAge());
        }
    }

    @Test
    public void findDtoByQueryProjection(){
        //when
        List<MemberDto> result = query
                .select(new QMemberDto(member.username, member.age))
                .from(member)
                .fetch();

        //then
        for (MemberDto memberDto : result) {
            System.out.println(memberDto);
        }
    }

    @Test
    public void dynamicQuery_BooleanBuilder() throws Exception{
        //given
        String usernameParam = "member1";
        Integer ageParam = 10;

        //when
        List<Member> result = searchMember(usernameParam, ageParam);

        //then
        assertEquals(result.size(), 1);
    }

    private List<Member> searchMember(String usernameCond, Integer ageCond) {
        BooleanBuilder builder = new BooleanBuilder();
        if (usernameCond != null){
            builder.and(member.username.eq(usernameCond));
        }

        if (ageCond != null){
            builder.and(member.age.eq(ageCond));
        }

        return query
                .selectFrom(member)
                .where(builder)
                .fetch();
    }

    @Test
    public void dynamicQuery_WhereParam() throws Exception{
        //given
        String usernameParam = "member1";
        Integer ageParam = 10;

        //when
        List<Member> result = searchMember2(usernameParam, ageParam);

        //then
        assertEquals(result.size(), 1);
    }

    private List<Member> searchMember2(String usernameCond, Integer ageCond) {
        return query
                .selectFrom(member)
//                .where(usernameEq(usernameCond), ageEq(ageCond))
                .where(allEq(usernameCond, ageCond))
                .fetch();

    }

    private BooleanExpression usernameEq(String usernameCond) {
        return usernameCond != null ? member.username.eq(usernameCond) : null;
    }

    private BooleanExpression ageEq(Integer ageCond) {
        return ageCond != null ? member.age.eq(ageCond) : null;
    }

    private BooleanExpression allEq(String usernameCond, Integer ageCond){
        return usernameEq(usernameCond).and(ageEq(ageCond));
    }

    @Test
    public void bulkUpdate() throws Exception{ // 벌크연산은 영속성 컨텍스트 무시하고 db를 바꾼다.
        //when
        //멤버1과 멤버 2가 나이가 각각 10세 20세이므로 비회원으로 바뀐다.
        long count = query
                .update(member)
                .set(member.username, "비회원")
                .where(member.age.lt(28))
                .execute();
        //then
        em.flush();
        em.clear();

    }

    @Test
    public void bulkAdd() throws Exception{
        //when
        long count = query
                .update(member)
                .set(member.age, member.age.add(1))
                .execute();
        //then
    }

    @Test
    public void bulkDelete() throws Exception{
        //when
        long count = query
                .delete(member)
                .where(member.age.gt(18))
                .execute();

        //then
    }
}
