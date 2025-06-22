-- GQLIntersectRule.sql
MATCH (a:person where a.id > 10)-[e]-> (b)
RETURN a, b
INTERSECT
MATCH (a:person where a.id < 20)-[e]-> (b)
RETURN a, b
;