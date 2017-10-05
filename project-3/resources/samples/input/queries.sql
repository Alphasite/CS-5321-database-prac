SELECT * FROM Sailors;
SELECT Sailors.A FROM Sailors;
SELECT Boats.F, Boats.D FROM Boats;
SELECT Reserves.G, Reserves.H FROM Reserves;
SELECT * FROM Sailors WHERE Sailors.B >= Sailors.C;
SELECT Sailors.A FROM Sailors WHERE Sailors.B >= Sailors.C
SELECT Sailors.A FROM Sailors WHERE Sailors.B >= Sailors.C AND Sailors.B < Sailors.C;
SELECT * FROM Sailors, Reserves WHERE Sailors.A = Reserves.G;
SELECT * FROM Sailors, Reserves, Boats WHERE Sailors.A = Reserves.G AND Reserves.H = Boats.D;
SELECT * FROM Sailors, Reserves, Boats WHERE Sailors.A = Reserves.G AND Reserves.H = Boats.D AND Sailors.B < 150;
SELECT DISTINCT * FROM Sailors;
SELECT * FROM Sailors S1, Sailors S2 WHERE S1.A < S2.A;
SELECT B.F, B.D FROM Boats B ORDER BY B.D;
SELECT * FROM Sailors S, Reserves R, Boats B WHERE S.A = R.G AND R.H = B.D ORDER BY S.C;
SELECT DISTINCT * FROM Sailors S, Reserves R, Boats B WHERE S.A = R.G AND R.H = B.D ORDER BY S.C;