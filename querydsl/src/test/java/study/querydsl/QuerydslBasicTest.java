package study.querydsl;

import com.querydsl.core.Tuple;
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
}
