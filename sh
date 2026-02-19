
grep -n "GridMovement\|PlaneNormal\|AreAnglesInverted" \
  /users/artem.kushner/dev/crboost_server/projects/zval_fixes/External/job003/warp_tiltseries/zval_fixes_Position_1.xml

grep -n "GridMovement\|PlaneNormal\|AreAnglesInverted" \
  /groups/klumpe/software/Setup/Testing/test1/run12/External/job003/warp_tiltseries/Position_1.xml


grep -n "GridCTF" \
  /users/artem.kushner/dev/crboost_server/projects/zval_fixes/External/job003/warp_tiltseries/zval_fixes_Position_1.xml | head -5

grep -n "GridCTF" \
  /groups/klumpe/software/Setup/Testing/test1/run12/External/job003/warp_tiltseries/Position_1.xml | head -5
